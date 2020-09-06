package chkpt;

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.savepoint.Savepoint;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;

import java.io.*;
import java.util.Iterator;
import java.util.Map;


/**
 * TODO 待测试
 * @Author lvkai
 * @Description
 *      读取checkpoint-xxx 目录下的 _metadata文件信息
 * @Date 2020/9/6 20:46
 **/
public class ParsingMetaData4CheckPointFile {
    public static void main(String[] args) throws IOException {
        // 读取metadata信息
        File f=new File("D:\\githubcom\\coding-flink\\upgrade\\src\\main\\resources\\_metadata");
        //第二步，建立管道,FileInputStream文件输入流类用于读文件
        FileInputStream fis=new FileInputStream(f);
        BufferedInputStream bis = new BufferedInputStream(fis);
        DataInputStream dis = new DataInputStream(bis);

        Savepoint savepoint = Checkpoints.loadCheckpointMetadata(dis,
                ParsingMetaData4CheckPointFile.class.getClassLoader());

        // 遍历 OperatorState，这里的每个 OperatorState 对应一个 Flink 任务的 Operator 算子
        // 不要与 OperatorState  和 KeyedState 混淆，不是一个层级的概念
        for (OperatorState operatorState : savepoint.getOperatorStates()) {
            System.out.println(operatorState);

            // 当前算子的状态大小为 0 ，表示算子不带状态，直接退出
            if (operatorState.getStateSize() == 0) continue;

            for (OperatorSubtaskState operatorSubtaskState : operatorState.getStates()) {

                // 解析 operatorSubtaskState 的 ManagedKeyedState
                parseManagedKeyedState(operatorSubtaskState);
                // 解析 operatorSubtaskState 的 ManagedOperatorState
                parseManagedOperatorState(operatorSubtaskState);
            }


        }

    }

    private static void parseManagedOperatorState(OperatorSubtaskState operatorSubtaskState) {
        for (OperatorStateHandle operatorStateHandle : operatorSubtaskState.getManagedOperatorState()) {
            StreamStateHandle delegateStateHandle = operatorStateHandle.getDelegateStateHandle();
            if (delegateStateHandle instanceof FileStateHandle){
                Path filePath = ((FileStateHandle) delegateStateHandle).getFilePath();
                System.out.println(filePath.getPath());
            }
        }
    }

    private static void parseManagedKeyedState(OperatorSubtaskState operatorSubtaskState) {
        // 遍历当前 subtask 的 KeyedState
        StateObjectCollection<KeyedStateHandle> managedKeyedState = operatorSubtaskState.getManagedKeyedState();
        for (KeyedStateHandle keyedStateHandle : managedKeyedState) {

            if (keyedStateHandle instanceof IncrementalKeyedStateHandle){
                // 针对 Flink RocksDB 的增量  因此仅处理 IncrementalRemoteKeyedStateHandle
                // 获取 RocksDB 的 sharedState
                Map<StateHandleID, StreamStateHandle> sharedState =
                        ((IncrementalRemoteKeyedStateHandle) keyedStateHandle).getSharedState();
                // 遍历所有的 sst 文件，key 为 sst 文件名，value 为对应的 hdfs 文件 Handle
                for(Map.Entry<StateHandleID,StreamStateHandle> entry:sharedState.entrySet()){
                    // 打印 sst 文件名
                    System.out.println("sstable 文件名：" + entry.getKey());
                    if(entry.getValue() instanceof FileStateHandle) {
                        Path filePath = ((FileStateHandle) entry.getValue()).getFilePath();
                        // 打印 sst 文件对应的 hdfs 文件位置
                        System.out.println("sstable文件对应的hdfs位置:" + filePath.getPath());
                    }
                }
            }else if(keyedStateHandle instanceof DirectoryKeyedStateHandle){
                DirectoryStateHandle directoryStateHandle = ((DirectoryKeyedStateHandle) keyedStateHandle).getDirectoryStateHandle();
                KeyGroupRange keyGroupRange = keyedStateHandle.getKeyGroupRange();
                System.out.println(directoryStateHandle.getDirectory());
                Iterator<Integer> iterator = keyGroupRange.iterator();
                while (iterator.hasNext()){
                    Integer next = iterator.next();
                    System.out.println(next);
                }
            }
        }



    }
}
