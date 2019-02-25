# DLedgerMmapFileStore

### 构造函数

首先简单了解下其构造函数，以便对其有一个初步的认识。

```java
public DLedgerMmapFileStore(DLedgerConfig dLedgerConfig, MemberState memberState) {
    // dledger配置
    this.dLedgerConfig = dLedgerConfig;
    // 成员状态，保存该节点作为集群成员的状态信息
    this.memberState = memberState;

    // 构造数据映射文件列表
    this.dataFileList = new MmapFileList(dLedgerConfig.getDataStorePath(), dLedgerConfig.getMappedFileSizeForEntryData());
    // 构造索引映射文件列表
    this.indexFileList = new MmapFileList(dLedgerConfig.getIndexStorePath(), dLedgerConfig.getMappedFileSizeForEntryIndex());

    localEntryBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(4 * 1024 * 1024));
    localIndexBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(INDEX_UNIT_SIZE * 2));

    // 构造刷盘服务线程
    flushDataService = new FlushDataService("DLedgerFlushDataService", logger);
    // 构造过期文件删除服务线程
    cleanSpaceService = new CleanSpaceService("DLedgerCleanSpaceService", logger);
}
```

### 成员变量

这里列出了 `DLedgerMmapFileStore` 主要的成员变量，只需要对其有一个大致的认识即可，再后面的源码分析中我们会再详细讲解。

TODO

### 启动DLedgerMmapFileStore

```java
public void startup() {
    // 加载映射文件
    load();
    // 对数据记录和对应的索引记录进行合法性和一致性检查。必要时，根据数据记录重建索引记录。并根据数据记录和检查点恢复节点的状态。
    recover();
    // 启动刷盘服务线程
    flushDataService.start();
    // 启动过期文件删除服务线程
    cleanSpaceService.start();
}
```

#### 加载映射文件

```java
public boolean load() {
    File dir = new File(this.storePath);
    File[] files = dir.listFiles();
    if (files != null) {
        // ascending order
        Arrays.sort(files);
        for (File file : files) {

            if (file.length() != this.mappedFileSize) {
                logger.warn(file + "\t" + file.length()
                    + " length not matched message store config value, please check it manually. You should delete old files before changing mapped file size");
                return false;
            }
            try {
                MmapFile mappedFile = new DefaultMmapFile(file.getPath(), mappedFileSize);

                mappedFile.setWrotePosition(this.mappedFileSize);
                mappedFile.setFlushedPosition(this.mappedFileSize);
                mappedFile.setCommittedPosition(this.mappedFileSize);
                this.mappedFiles.add(mappedFile);
                logger.info("load " + file.getPath() + " OK");
            } catch (IOException e) {
                logger.error("load file " + file + " error", e);
                return false;
            }
        }
    }

    return true;
}
```

上述代码实现很简单，最关键的代码就是通过磁盘文件来构造 `DefaultMmapFile`。

```java
public DefaultMmapFile(final String fileName, final int fileSize) throws IOException {
    this.fileName = fileName;
    this.fileSize = fileSize;
    this.file = new File(fileName);
    // 设置映射文件的起始偏移量为文件名
    this.fileFromOffset = Long.parseLong(this.file.getName());
    boolean ok = false;

    // 判断父目录是否存在，如果不存在则创建父目录
    ensureDirOK(this.file.getParent());

    try {
        // 对file进行包装，以支持其随机读写
        this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
        // fileChannel的内存映射对象，将此通道的文件区域直接映射到内存中。
        this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
        TOTAL_MAPPED_FILES.incrementAndGet();
        ok = true;
    } catch (FileNotFoundException e) {
        logger.error("create file channel " + this.fileName + " Failed. ", e);
        throw e;
    } catch (IOException e) {
        logger.error("map file " + this.fileName + " Failed. ", e);
        throw e;
    } finally {
        if (!ok && this.fileChannel != null) {
            this.fileChannel.close();
        }
    }
}
```

#### 状态恢复

这里的代码实现逻辑较为复杂，主要对数据记录和对应的索引记录进行合法性和一致性检查。必要时，根据数据记录重建索引记录。并根据数据记录和检查点恢复节点的状态。

总结起来有以下几点：

s1. 检查映射文件的完整性。

```text
检查数据映射文件和索引映射文件的数据完整性。除当前正在写入数据的映射文件外，其它映射文件应该满足 `MmapFileList#mappedFileSize` 大小。
```

s2. 如果数据映射文件列表为空，则删除所有索引数据，并返回；否则继续执行下面的步骤。

```text
该步骤主要执行更新索引映射文件的全局刷盘位置和提交位置，将它们设置为0；销毁所有的索引映射文件。
```

s3. 预检查。逆序检查所有数据映射文件的首条数据记录和对应的索引记录，直至找到一个符合预期的数据映射文件。

```text
所谓符合预期是指，其首条数据记录和对应的索引记录的数据合法且一致。

* 读取当前映射文件的第一条数据记录；
* 数据记录的合法性校验；
* 读取该数据记录所对应的索引记录；
* 索引记录的合法性校验；
* 保存找到的合法的数据记录的 `index`，将其作为 `firstEntryIndex`；
```

s4. 遍历上一步查找到的数据映射文件以及后续文件，检查其数据记录和索引记录的合法性。如果数据不合法，则根据数据记录重建索引记录。

* 如果遍历到当前映射文件末尾，则继续下一映射文件；当没有可继续遍历的映射文件时退出循环；
* 读取当前映射文件的一条数据记录；
* 数据记录的合法性校验；
* 定位到下一条数据记录；
* 读取该数据记录所对应的索引记录；
* 索引记录的合法性校验；
* 如果数据记录和对应的索引记录的数据不匹配，则删除该索引记录（包含）之后的所有索引数据，并设置启用根据数据记录重建索引记录；
* 根据数据记录重建索引记录；
* 记录当前已遍历的记录的index和term，以及当前待处理的位置（下一次要处理的全局偏移量）；

s5. 设置 `ledgerEndIndex`、`ledgerBeginIndex`、`ledgerEndTerm`。

```text
如有必要，会删除 `ledgerBeginIndex` 前面的无效索引数据。
因为有可能前面的这些索引记录对应的数据记录并不存在，所以需要将这些索引删除。
```

s6. 更新 `dataFileList` 的 `flushedWhere`、`committedWhere`。

```text
TODO
```

s7. 更新 `indexFileList` 的 `flushedWhere`、`committedWhere`。

```text
如有必要，会删除 `ledgerEndIndex` 后面的无效索引数据。
有可能后面还有一些索引记录其对应的数据记录并不存在，所以需要将这些索引删除。
```

s8. 更新节点状态。

```text
更新节点的最后一条记录的 `index` 和 `term`。
```

s9. 从检查点文件中加载 `committedIndex`，并更新 `committedIndex`、`committedPos`。

```java
public void recover() {
    if (!hasRecovered.compareAndSet(false, true)) {
        return;
    }

    // 1. 检查映射文件的完整性
    PreConditions.check(dataFileList.checkSelf(), DLedgerResponseCode.DISK_ERROR, "check data file order failed before recovery");
    PreConditions.check(indexFileList.checkSelf(), DLedgerResponseCode.DISK_ERROR, "check index file order failed before recovery");

    // 2. 如果数据映射文件列表为空，则删除所有索引记录，并返回；否则继续执行下面的步骤。
    final List<MmapFile> mappedFiles = this.dataFileList.getMappedFiles();
    if (mappedFiles.isEmpty()) {
        // 更新索引映射文件的全局刷盘位置和提交位置，将它们设置为0
        this.indexFileList.updateWherePosition(0);
        // 销毁所有的索引映射文件
        this.indexFileList.truncateOffset(0);
        return;
    }

    MmapFile lastMappedFile = dataFileList.getLastMappedFile();

    // 只向前检查3个文件
    // 这是个经验值
    int index = mappedFiles.size() - 3;
    if (index < 0) {
        index = 0;
    }

    long firstEntryIndex = -1;

    // 3. 预检查
    // 逆序检查所有数据映射文件的首条数据记录和对应的索引记录，直至找到一个符合预期的数据映射文件（其首条数据记录和对应的索引记录的数据合法且一致）。
    for (int i = index; i >= 0; i--) {
        index = i;
        MmapFile mappedFile = mappedFiles.get(index);
        ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
        try {
            long startPos = mappedFile.getFileFromOffset(); // 当前映射文件的起始偏移量

            // 3.1 读取当前映射文件的第一条数据记录
            int magic = byteBuffer.getInt();
            int size = byteBuffer.getInt(); // 记录的总字节数
            long entryIndex = byteBuffer.getLong(); // 记录的索引位置
            long entryTerm = byteBuffer.getLong();
            long pos = byteBuffer.getLong(); // 记录的起始偏移量
            byteBuffer.getInt(); // channel
            byteBuffer.getInt(); // chain crc
            byteBuffer.getInt(); // body crc
            int bodySize = byteBuffer.getInt(); // 记录中body的字节数

            // 3.2 数据记录的合法性校验
            // 记录的magic code检查，如果是映射文件的结束标识，则抛出异常
            PreConditions.check(magic != MmapFileList.BLANK_MAGIC_CODE && magic >= MAGIC_1 && MAGIC_1 <= CURRENT_MAGIC, DLedgerResponseCode.DISK_ERROR, "unknown magic=%d", magic);
            // 记录的总字节数检查，应该大于header的字节数
            PreConditions.check(size > DLedgerEntry.HEADER_SIZE, DLedgerResponseCode.DISK_ERROR, "Size %d should > %d", size, DLedgerEntry.HEADER_SIZE);
            // 记录的起始偏移量检查，因为是当前映射文件的第一条记录，所以应该满足等于startPos
            PreConditions.check(pos == startPos, DLedgerResponseCode.DISK_ERROR, "pos %d != %d", pos, startPos);
            // 记录中body的字节数检查，body的字节数加上body的相对偏移量应当等于记录的总字节数
            PreConditions.check(bodySize + DLedgerEntry.BODY_OFFSET == size, DLedgerResponseCode.DISK_ERROR, "size %d != %d + %d", size, bodySize, DLedgerEntry.BODY_OFFSET);

            // 3.3 读取该数据记录所对应的索引记录
            SelectMmapBufferResult indexSbr = indexFileList.getData(entryIndex * INDEX_UNIT_SIZE);
            PreConditions.check(indexSbr != null, DLedgerResponseCode.DISK_ERROR, "index=%d pos=%d", entryIndex, entryIndex * INDEX_UNIT_SIZE);
            indexSbr.release();

            ByteBuffer indexByteBuffer = indexSbr.getByteBuffer();
            int magicFromIndex = indexByteBuffer.getInt();
            long posFromIndex = indexByteBuffer.getLong();
            int sizeFromIndex = indexByteBuffer.getInt();
            long indexFromIndex = indexByteBuffer.getLong();
            long termFromIndex = indexByteBuffer.getLong();

            // 3.4 索引记录的合法性校验
            PreConditions.check(magic == magicFromIndex, DLedgerResponseCode.DISK_ERROR, "magic %d != %d", magic, magicFromIndex);
            PreConditions.check(size == sizeFromIndex, DLedgerResponseCode.DISK_ERROR, "size %d != %d", size, sizeFromIndex);
            PreConditions.check(entryIndex == indexFromIndex, DLedgerResponseCode.DISK_ERROR, "index %d != %d", entryIndex, indexFromIndex);
            PreConditions.check(entryTerm == termFromIndex, DLedgerResponseCode.DISK_ERROR, "term %d != %d", entryTerm, termFromIndex);
            PreConditions.check(posFromIndex == mappedFile.getFileFromOffset(), DLedgerResponseCode.DISK_ERROR, "pos %d != %d", mappedFile.getFileFromOffset(), posFromIndex);

            // 3.5 保存找到的合法的数据记录的index，将其作为firstEntryIndex
            firstEntryIndex = entryIndex;
            break;
        } catch (Throwable t) {
            logger.warn("Pre check data and index failed {}", mappedFile.getFileName(), t);
        }
    }

    // 4. 遍历上一步查找到的数据映射文件以及后续文件，检查其数据记录和索引记录的合法性。如果数据不合法，则根据数据记录重建索引记录。
    MmapFile mappedFile = mappedFiles.get(index);
    ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
    logger.info("Begin to recover data from entryIndex={} fileIndex={} fileSize={} fileName={} ", firstEntryIndex, index, mappedFiles.size(), mappedFile.getFileName());

    long lastEntryIndex = -1; // 当前已遍历记录的index
    long lastEntryTerm = -1; // 当前已遍历记录的任期
    long processOffset = mappedFile.getFileFromOffset(); // 当前待处理的位置（全局偏移量）

    boolean needWriteIndex = false; // 是否需要根据数据记录重建索引记录

    while (true) {
        try {

            int relativePos = byteBuffer.position(); // 当前遍历的数据记录的相对偏移量
            long absolutePos = mappedFile.getFileFromOffset() + relativePos; // 当前遍历的数据记录的全局偏移量

            // 4.1 如果遍历到当前映射文件末尾，则继续下一映射文件；当没有可继续遍历的映射文件时退出循环。
            int magic = byteBuffer.getInt(); // magic code
            if (magic == MmapFileList.BLANK_MAGIC_CODE) {
                processOffset = mappedFile.getFileFromOffset() + mappedFile.getFileSize(); // 定位到下一个映射文件的起始位置
                index++;
                if (index >= mappedFiles.size()) {
                    logger.info("Recover data file over, the last file {}", mappedFile.getFileName());
                    break;
                } else {
                    mappedFile = mappedFiles.get(index);
                    byteBuffer = mappedFile.sliceByteBuffer();
                    processOffset = mappedFile.getFileFromOffset();
                    logger.info("Trying to recover data file {}", mappedFile.getFileName());
                    continue;
                }
            }

            // 4.2 读取当前映射文件的一条数据记录
            int size = byteBuffer.getInt();
            long entryIndex = byteBuffer.getLong();
            long entryTerm = byteBuffer.getLong();
            long pos = byteBuffer.getLong();
            byteBuffer.getInt(); // channel
            byteBuffer.getInt(); // chain crc
            byteBuffer.getInt(); // body crc
            int bodySize = byteBuffer.getInt();

            // 4.3 数据记录的合法性校验
            // 记录的起始偏移量检查
            PreConditions.check(pos == absolutePos, DLedgerResponseCode.DISK_ERROR, "pos %d != %d", pos, absolutePos);
            // 记录中body的字节数检查，body的字节数加上body的相对偏移量应当等于记录的总字节数
            PreConditions.check(bodySize + DLedgerEntry.BODY_OFFSET == size, DLedgerResponseCode.DISK_ERROR, "size %d != %d + %d", size, bodySize, DLedgerEntry.BODY_OFFSET);
            // 记录的magic code检查，如果是映射文件的结束标识，则抛出异常
            PreConditions.check(magic <= CURRENT_MAGIC && magic >= MAGIC_1, DLedgerResponseCode.DISK_ERROR, "pos=%d size=%d magic=%d index=%d term=%d currMagic=%d", absolutePos, size, magic, entryIndex, entryTerm, CURRENT_MAGIC);
            // 记录的index检查，index应该是单调递增
            if (lastEntryIndex != -1) {
                PreConditions.check(entryIndex == lastEntryIndex + 1, DLedgerResponseCode.DISK_ERROR, "pos=%d size=%d magic=%d index=%d term=%d lastEntryIndex=%d", absolutePos, size, magic, entryIndex, entryTerm, lastEntryIndex);
            }
            // 记录的任期term检查
            PreConditions.check(entryTerm >= lastEntryTerm, DLedgerResponseCode.DISK_ERROR, "pos=%d size=%d magic=%d index=%d term=%d lastEntryTerm=%d ", absolutePos, size, magic, entryIndex, entryTerm, lastEntryTerm);
            // 记录的总字节数检查，应该大于header的字节数
            PreConditions.check(size > DLedgerEntry.HEADER_SIZE, DLedgerResponseCode.DISK_ERROR, "size %d should > %d", size, DLedgerEntry.HEADER_SIZE);

            // 4.4 定位到下一条记录
            byteBuffer.position(relativePos + size);

            if (!needWriteIndex) {
                try {
                    // 4.5 读取该数据记录所对应的索引记录
                    SelectMmapBufferResult indexSbr = indexFileList.getData(entryIndex * INDEX_UNIT_SIZE);
                    PreConditions.check(indexSbr != null, DLedgerResponseCode.DISK_ERROR, "index=%d pos=%d", entryIndex, entryIndex * INDEX_UNIT_SIZE);
                    indexSbr.release();

                    ByteBuffer indexByteBuffer = indexSbr.getByteBuffer();
                    int magicFromIndex = indexByteBuffer.getInt();
                    long posFromIndex = indexByteBuffer.getLong();
                    int sizeFromIndex = indexByteBuffer.getInt();
                    long indexFromIndex = indexByteBuffer.getLong();
                    long termFromIndex = indexByteBuffer.getLong();

                    // 4.6 索引记录的合法性校验
                    PreConditions.check(magic == magicFromIndex, DLedgerResponseCode.DISK_ERROR, "magic %d != %d", magic, magicFromIndex);
                    PreConditions.check(size == sizeFromIndex, DLedgerResponseCode.DISK_ERROR, "size %d != %d", size, sizeFromIndex);
                    PreConditions.check(entryIndex == indexFromIndex, DLedgerResponseCode.DISK_ERROR, "index %d != %d", entryIndex, indexFromIndex);
                    PreConditions.check(entryTerm == termFromIndex, DLedgerResponseCode.DISK_ERROR, "term %d != %d", entryTerm, termFromIndex);
                    PreConditions.check(absolutePos == posFromIndex, DLedgerResponseCode.DISK_ERROR, "pos %d != %d", mappedFile.getFileFromOffset(), posFromIndex);
                } catch (Throwable t) {
                    // 4.7 数据记录和对应的索引记录的数据不匹配

                    logger.warn("Compare data to index failed {}", mappedFile.getFileName(), t);

                    // 删除该索引记录（包含）之后的所有索引记录，会销毁其后的所有索引映射文件
                    indexFileList.truncateOffset(entryIndex * INDEX_UNIT_SIZE);

                    // 重建索引映射文件的准备工作。
                    // 1. 删除所有索引映射文件；
                    // 2. 获取一个新的映射文件；
                    // 3. 设置该映射文件的wrotePosition、committedPosition、flushedPosition、startPosition为pos % mappedFileSize
                    if (indexFileList.getMaxWrotePosition() != entryIndex * INDEX_UNIT_SIZE) {
                        long truncateIndexOffset = entryIndex * INDEX_UNIT_SIZE;
                        logger.warn("[Recovery] rebuild for index wrotePos={} not equal to truncatePos={}", indexFileList.getMaxWrotePosition(), truncateIndexOffset);
                        PreConditions.check(indexFileList.rebuildWithPos(truncateIndexOffset), DLedgerResponseCode.DISK_ERROR, "rebuild index truncatePos=%d", truncateIndexOffset);
                    }

                    // 设置启用根据数据记录重建索引记录
                    needWriteIndex = true;
                }
            }

            // 4.8 根据数据记录重建索引记录
            if (needWriteIndex) {
                ByteBuffer indexBuffer = localIndexBuffer.get();
                DLedgerEntryCoder.encodeIndex(absolutePos, size, magic, entryIndex, entryTerm, indexBuffer);
                // 向索引映射文件中追加索引记录，并返回写入之前的写位置。
                // 如果映射文件可用空间不足，则失败，返回-1。
                long indexPos = indexFileList.append(indexBuffer.array(), 0, indexBuffer.remaining(), false);
                PreConditions.check(indexPos == entryIndex * INDEX_UNIT_SIZE, DLedgerResponseCode.DISK_ERROR, "Write index failed index=%d", entryIndex);
            }

            // 4.9 记录当前已遍历的记录的index和term，以及当前待处理的位置（下一次要处理的全局偏移量）
            lastEntryIndex = entryIndex;
            lastEntryTerm = entryTerm;
            processOffset += size;

        } catch (Throwable t) {
            logger.info("Recover data file to the end of {} ", mappedFile.getFileName(), t);
            break;
        }
    }

    logger.info("Recover data to the end entryIndex={} processOffset={} lastFileOffset={} cha={}",
            lastEntryIndex, processOffset, lastMappedFile.getFileFromOffset(), processOffset - lastMappedFile.getFileFromOffset());
    // 如果机器异常宕机，磁盘上的数据就可能是不完整的。比如，一条消息，只刷盘了一部分。
    // 如果某条消息只刷盘了一部分，这会导致在前面的数据记录合法性检查中失败，从而退出整个检查过程。
    // 并在后面的逻辑中删除这条消息以及后续消息。这里就是在删除前做一下检查，如果删除的数据太多，就需要人工干预了。
    if (lastMappedFile.getFileFromOffset() - processOffset > lastMappedFile.getFileSize()) {
        logger.error("[MONITOR]The processOffset is too small, you should check it manually before truncating the data from {}", processOffset);
        System.exit(-1);
    }

    // 5.1 设置ledgerEndIndex、ledgerEndTerm
    ledgerEndIndex = lastEntryIndex;
    ledgerEndTerm = lastEntryTerm;

    if (lastEntryIndex != -1) {
        // 复检最后一条数据记录
        DLedgerEntry entry = get(lastEntryIndex);
        PreConditions.check(entry != null, DLedgerResponseCode.DISK_ERROR, "recheck get null entry");
        PreConditions.check(entry.getIndex() == lastEntryIndex, DLedgerResponseCode.DISK_ERROR, "recheck index %d != %d", entry.getIndex(), lastEntryIndex);

        // 5.2 设置ledgerBeginIndex。
        // 如有必要，会删除ledgerBeginIndex前面的无效索引记录。
        // 有可能前面的这些索引记录对应的数据记录并不存在，所以需要将这些索引删除。
        reviseLedgerBeginIndex();
    }

    // 6. 更新dataFileList的flushedWhere、committedWhere
    this.dataFileList.updateWherePosition(processOffset);
    // 如有必要，会删除processOffset后面的无效数据记录。
    // 前面我们提到了，如果机器异常宕机，磁盘上的数据就可能是不完整的。比如，一条消息，只刷盘了一部分。
    // 这时就需要删除这条消息以及后续消息。
    this.dataFileList.truncateOffset(processOffset);

    long indexProcessOffset = (lastEntryIndex + 1) * INDEX_UNIT_SIZE;
    // 7. 更新indexFileList的flushedWhere、committedWhere
    this.indexFileList.updateWherePosition(indexProcessOffset);
    this.indexFileList.truncateOffset(indexProcessOffset);

    // 8. 更新节点的最后一条记录的index和term
    updateLedgerEndIndexAndTerm();

    PreConditions.check(dataFileList.checkSelf(), DLedgerResponseCode.DISK_ERROR, "check data file order failed after recovery");
    PreConditions.check(indexFileList.checkSelf(), DLedgerResponseCode.DISK_ERROR, "check index file order failed after recovery");

    // 9. 从检查点文件中加载committedIndex，并更新committedIndex、committedPos
    // 加载committedIndex
    // Load the committed index from checkpoint
    Properties properties = loadCheckPoint();
    if (properties == null || !properties.containsKey(COMMITTED_INDEX_KEY)) {
        return;
    }
    String committedIndexStr = String.valueOf(properties.get(COMMITTED_INDEX_KEY)).trim();
    if (committedIndexStr.length() <= 0) {
        return;
    }
    logger.info("Recover to get committed index={} from checkpoint", committedIndexStr);
    // 更新committedIndex、committedPos
    updateCommittedIndex(memberState.currTerm(), Long.valueOf(committedIndexStr));

    return;
}
```

\(1\) 为什么只向前检查3个文件？

```java
int index = mappedFiles.size() - 3;
if (index < 0) {
    index = 0;
}
```

作者说这是个经验之谈。如果要较真的话，是需要完整检查的。但是那样的话，启动时间就很长了。业界还有另外一种做法，启动时不做全部检查，但是在发现错误时，自动利用副本恢复。DLedger会采取后面这种方式。

![-w798](http://pm5ge6g6l.bkt.clouddn.com/15508182230015.jpg)

\(2\) 数据记录和对应的索引记录的数据不匹配如何处理？

如果数据记录和对应的索引记录的数据不匹配，则需要根据数据记录重建索引记录。

这里分两种情况需要考虑，

第一种，单纯只是考虑不匹配时，必然首先要做的就是删除当前 entryIndex 以及后续所有的索引记录。

```java
// DLedgerMmapFileStore.java
// 删除该索引记录（包含）之后的所有索引记录，会销毁其后的所有索引映射文件
indexFileList.truncateOffset(entryIndex * INDEX_UNIT_SIZE);
```

```java
// MmapFileList.java
/**
 * 删除指定偏移量之后的所有数据，会销毁其后的所有映射文件
 *
 * @param offset 全局偏移量
 */
public void truncateOffset(long offset) {
    Object[] mfs = this.copyMappedFiles();
    if (mfs == null) {
        return;
    }
    List<MmapFile> willRemoveFiles = new ArrayList<MmapFile>();

    for (int i = 0; i < mfs.length; i++) {
        MmapFile file = (MmapFile) mfs[i];
        long fileTailOffset = file.getFileFromOffset() + this.mappedFileSize; // 当前映射文件的末尾全局偏移量
        if (fileTailOffset > offset) {
            if (offset >= file.getFileFromOffset()) {
                file.setWrotePosition((int) (offset % this.mappedFileSize));
                file.setCommittedPosition((int) (offset % this.mappedFileSize));
                file.setFlushedPosition((int) (offset % this.mappedFileSize));
            } else {
                willRemoveFiles.add(file);
            }
        }
    }

    // 销毁指定的映射文件
    this.destroyExpiredFiles(willRemoveFiles);
    // 将指定的映射文件从mappedFiles中移除
    this.deleteExpiredFiles(willRemoveFiles);
}
```

然后设置启用根据数据记录重建索引记录。

```java
needWriteIndex = true;
```

然后再下面通过数据记录来重建索引记录。

```java
if (needWriteIndex) {
    ByteBuffer indexBuffer = localIndexBuffer.get();
    DLedgerEntryCoder.encodeIndex(absolutePos, size, magic, entryIndex, entryTerm, indexBuffer);
    // 向索引映射文件中追加索引记录，并返回写入之前的写位置。
    // 如果映射文件可用空间不足，则失败，返回-1。
    long indexPos = indexFileList.append(indexBuffer.array(), 0, indexBuffer.remaining(), false);
    PreConditions.check(indexPos == entryIndex * INDEX_UNIT_SIZE, DLedgerResponseCode.DISK_ERROR, "Write index failed index=%d", entryIndex);
}
```

第二种情况，试想一下下面的场景：

假设集群中有3个副本节点 A、B、C，启动集群，并发送消息，然后停掉副本节点 A，继续发送很多消息。一段相当长时间后，你想快速恢复副本节点 A（因为此时 A 已经落后很多，它自身的数据也都已经过期了），就可以删除副本节点 A 原有的 commitlog，然后从其它副本节点（如 B）拷贝最后几个 committlog 过去，就可以触发这段代码了。

具体的是这样子的，上述只是拷贝了数据文件，并没有拷贝索引文件。考虑到过期记录会被删除，所以此时的副本节点 A 和 B 的数据如下：

![-w908](http://pm5ge6g6l.bkt.clouddn.com/15508222434752.jpg)

很显然，当副本节点 A 启动时，读取到 `entryIndex` 为10，其相应的索引记录并不存在，即 `indexFileList.getMaxWrotePosition() != entryIndex * INDEX_UNIT_SIZE` 成立。此时，只需要将原有的所有索引记录全部删除，重新根据数据记录构建。

```java
if (indexFileList.getMaxWrotePosition() != entryIndex * INDEX_UNIT_SIZE) {
    long truncateIndexOffset = entryIndex * INDEX_UNIT_SIZE;
    logger.warn("[Recovery] rebuild for index wrotePos={} not equal to truncatePos={}", indexFileList.getMaxWrotePosition(), truncateIndexOffset);
    PreConditions.check(indexFileList.rebuildWithPos(truncateIndexOffset), DLedgerResponseCode.DISK_ERROR, "rebuild index truncatePos=%d", truncateIndexOffset);
}
```

\(3\) 人工干预

```java
if (lastMappedFile.getFileFromOffset() - processOffset > lastMappedFile.getFileSize()) {
    logger.error("[MONITOR]The processOffset is too small, you should check it manually before truncating the data from {}", processOffset);
    System.exit(-1);
}
```

看代码的日志打印就明白它的用意了。

> The processOffset is too small, you should check it manually before truncating the data from processOffset.
>
> `processOffset` 太小了，你应该在删除 `processOffset` 之后的数据记录之前人工干预。

在后面的逻辑中，会通过 `this.dataFileList.truncateOffset(processOffset);` 删除无效的数据记录。

如果机器异常宕机，磁盘上的数据就可能是不完整的。比如，一条消息，只刷盘了一部分。那么这条消息以及后续消息都要截掉，然后从其它副本拷贝一份过来。截得太多了，要慎重，建议是人工干预。

通俗点讲，如果某条消息只刷盘了一部分，这会导致在前面的数据记录合法性检查中失败，从而退出整个检查过程。并在后面的逻辑中删除这条消息以及后续消息。这里就是在删除前做一下检查，如果删除的数据太多，就需要人工干预了。

### ShutdownAbleThread

继承 `Thread`，提供服务线程的基本抽象。为服务线程实现了关闭逻辑，并提供了等待通知模式。

```java
public abstract class ShutdownAbleThread extends Thread {

    /**
     * 用于实现等待通知模式
     */
    protected final ResettableCountDownLatch waitPoint = new ResettableCountDownLatch(1);

    protected Logger logger;

    /**
     * 用于标记是否收到唤醒的通知
     */
    protected volatile AtomicBoolean hasNotified = new AtomicBoolean(false);

    /**
     * 线程运行状态
     */
    private AtomicBoolean running = new AtomicBoolean(true);

    /**
     * 用于通知线程已关闭
     */
    private CountDownLatch latch = new CountDownLatch(1);

    public ShutdownAbleThread(String name, Logger logger) {
        super(name);
        this.logger = logger;

    }

    /**
     * 关闭线程，会超时等待直至线程执行结束
     */
    public void shutdown() {
        if (running.compareAndSet(true, false)) {
            try {
                wakeup();
                latch.await(10, TimeUnit.SECONDS); // 超时等待线程关闭
            } catch (Throwable t) {
                if (logger != null) {
                    logger.error("Unexpected Error in shutting down {} ", getName(), t);
                }
            }
            if (latch.getCount() != 0) {
                if (logger != null) {
                    logger.error("The {} failed to shutdown in {} seconds", getName(), 10);
                }

            }
        }
    }

    public abstract void doWork();

    /**
     * 线程等待通知模式 —— 通知，唤醒
     */
    public void wakeup() {
        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }
    }

    /**
     * 线程等待通知模式 —— 等待
     *
     * @param interval 超时等待的时间，单位毫秒
     * @throws InterruptedException
     */
    public void waitForRunning(long interval) throws InterruptedException {
        // 已收到唤醒的通知，不需要再进入超时等待，直接返回
        if (hasNotified.compareAndSet(true, false)) {
            return;
        }

        // entry to wait
        waitPoint.reset();

        try {
            waitPoint.await(interval, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.error("The {} is interrupted", getName(), e);
            throw e;
        } finally {
            hasNotified.set(false); // 重置唤醒标记
        }
    }

    /**
     * 线程执行逻辑
     */
    public void run() {
        while (running.get()) {
            try {
                doWork();
            } catch (Throwable t) {
                if (logger != null) {
                    logger.error("Unexpected Error in running {} ", getName(), t);
                }
            }
        }
        latch.countDown();
    }

    public Logger getLogger() {
        return logger;
    }

    public void setLogger(Logger logger) {
        this.logger = logger;
    }

}
```

### FlushDataService

刷盘服务线程主要执行以下几个工作：

1. 执行数据映射文件和索引映射文件刷盘；
2. 每隔三秒持久化 `ledgerEndIndex`、`committedIndex` 到磁盘；
3. 超时等待刷盘通知，如果10毫秒没有收到通知，会再次执行刷盘；

```java
class FlushDataService extends ShutdownAbleThread {

    public FlushDataService(String name, Logger logger) {
        super(name, logger);
    }

    @Override
    public void doWork() {
        try {
            long start = System.currentTimeMillis();

            // 执行数据映射文件和索引映射文件刷盘
            DLedgerMmapFileStore.this.dataFileList.flush(0);
            DLedgerMmapFileStore.this.indexFileList.flush(0);

            if (DLedgerUtils.elapsed(start) > 500) {
                logger.info("Flush data cost={} ms", DLedgerUtils.elapsed(start));
            }

            // checkPointInterval默认3秒
            // 每隔三秒持久化ledgerEndIndex、committedIndex到磁盘
            if (DLedgerUtils.elapsed(lastCheckPointTimeMs) > dLedgerConfig.getCheckPointInterval()) {
                persistCheckPoint();
                lastCheckPointTimeMs = System.currentTimeMillis();
            }

            // flushFileInterval默认10毫秒
            // 等待刷盘通知
            waitForRunning(dLedgerConfig.getFlushFileInterval());
        } catch (Throwable t) {
            logger.info("Error in {}", getName(), t);
            DLedgerUtils.sleep(200);
        }
    }
}
```

### CleanSpaceService

文件清理服务线程。在每天的凌晨04点或者磁盘不足时删除过期的文件，极端情况下会执行强制删除。每次最多删除10个文件。

```java
class CleanSpaceService extends ShutdownAbleThread {

    double storeBaseRatio = DLedgerUtils.getDiskPartitionSpaceUsedPercent(dLedgerConfig.getStoreBaseDir());
    double dataRatio = DLedgerUtils.getDiskPartitionSpaceUsedPercent(dLedgerConfig.getDataStorePath());

    public CleanSpaceService(String name, Logger logger) {
        super(name, logger);
    }

    @Override
    public void doWork() {
        try {
            storeBaseRatio = DLedgerUtils.getDiskPartitionSpaceUsedPercent(dLedgerConfig.getStoreBaseDir());
            dataRatio = DLedgerUtils.getDiskPartitionSpaceUsedPercent(dLedgerConfig.getDataStorePath());
            long hourOfMs = 3600L * 1000L; // 1 hour
            long fileReservedTimeMs = dLedgerConfig.getFileReservedHours() * hourOfMs; // 72 hour
            if (fileReservedTimeMs < hourOfMs) {
                logger.warn("The fileReservedTimeMs={} is smaller than hourOfMs={}", fileReservedTimeMs, hourOfMs);
                fileReservedTimeMs = hourOfMs;
            }

            // If the disk is full, should prevent more data to get in
            DLedgerMmapFileStore.this.isDiskFull = isNeedForbiddenWrite(); // 磁盘是否写满
            boolean timeUp = isTimeToDelete(); // 是否到了删除文件的时间
            boolean checkExpired = isNeedCheckExpired(); // 是否需要检查文件过期
            boolean forceClean = isNeedForceClean(); // 是否执行强制删除文件
            boolean enableForceClean = dLedgerConfig.isEnableDiskForceClean(); // 是否启用强制删除文件

            // 到了删除文件的时间，或者需要检查文件过期
            if (timeUp || checkExpired) {
                // 删除过期的文件。
                // 如果启用强制删除，则无论文件是否过期都删除。
                int count = getDataFileList().deleteExpiredFileByTime(fileReservedTimeMs, 100, 120 * 1000, forceClean && enableForceClean);
                if (count > 0 || (forceClean && enableForceClean) || isDiskFull) {
                    logger.info("Clean space count={} timeUp={} checkExpired={} forceClean={} enableForceClean={} diskFull={} storeBaseRatio={} dataRatio={}",
                            count, timeUp, checkExpired, forceClean, enableForceClean, isDiskFull, storeBaseRatio, dataRatio);
                }
                if (count > 0) {
                    // 设置ledgerBeginIndex，并删除ledgerBeginIndex前面的无效索引数据。
                    DLedgerMmapFileStore.this.reviseLedgerBeginIndex();
                }
            }

            // 超时等待文件清理通知
            waitForRunning(100);
        } catch (Throwable t) {
            logger.info("Error in {}", getName(), t);
            DLedgerUtils.sleep(200);
        }
    }

    /**
     * 是否到了删除文件的时间
     *
     * @return
     */
    private boolean isTimeToDelete() {
        String when = DLedgerMmapFileStore.this.dLedgerConfig.getDeleteWhen();
        if (DLedgerUtils.isItTimeToDo(when)) {
            return true;
        }

        return false;
    }

    /**
     * 是否需要检查文件过期
     *
     * @return
     */
    private boolean isNeedCheckExpired() {
        if (storeBaseRatio > dLedgerConfig.getDiskSpaceRatioToCheckExpired()
                || dataRatio > dLedgerConfig.getDiskSpaceRatioToCheckExpired()) {
            return true;
        }
        return false;
    }

    /**
     * 是否执行强制删除文件
     *
     * @return
     */
    private boolean isNeedForceClean() {
        if (storeBaseRatio > dLedgerConfig.getDiskSpaceRatioToForceClean()
                || dataRatio > dLedgerConfig.getDiskSpaceRatioToForceClean()) {
            return true;
        }
        return false;
    }

    /**
     * 磁盘是否写满
     *
     * @return
     */
    private boolean isNeedForbiddenWrite() {
        if (storeBaseRatio > dLedgerConfig.getDiskFullRatio()
                || dataRatio > dLedgerConfig.getDiskFullRatio()) {
            return true;
        }
        return false;
    }
}
```

