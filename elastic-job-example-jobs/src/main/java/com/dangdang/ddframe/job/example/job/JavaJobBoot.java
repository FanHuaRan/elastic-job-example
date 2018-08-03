package com.dangdang.ddframe.job.example.job;

import com.dangdang.ddframe.job.config.JobCoreConfiguration;
import com.dangdang.ddframe.job.config.JobRootConfiguration;
import com.dangdang.ddframe.job.config.dataflow.DataflowJobConfiguration;
import com.dangdang.ddframe.job.config.simple.SimpleJobConfiguration;
import com.dangdang.ddframe.job.example.job.dataflow.JavaDataflowJob;
import com.dangdang.ddframe.job.example.job.simple.JavaSimpleJob;
import com.dangdang.ddframe.job.executor.ShardingContexts;
import com.dangdang.ddframe.job.lite.api.JobScheduler;
import com.dangdang.ddframe.job.lite.api.listener.ElasticJobListener;
import com.dangdang.ddframe.job.lite.config.LiteJobConfiguration;
import com.dangdang.ddframe.job.lite.internal.schedule.JobScheduleController;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import com.dangdang.ddframe.job.reg.zookeeper.ZookeeperConfiguration;
import com.dangdang.ddframe.job.reg.zookeeper.ZookeeperRegistryCenter;
import org.quartz.JobListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Fan Huaran
 * created on 2018/8/3
 * @description
 */
public class JavaJobBoot {
    private static final Logger logger = LoggerFactory.getLogger(JavaSimpleJob.class);

    // 定义zookeeper注册中心配置对象
    private static final ZookeeperConfiguration zookeeperConfiguration = createZookeeperConfiguration();

    // 定义Zookeeper注册中心
    private static final CoordinatorRegistryCenter coordinatorRegistryCenter = new ZookeeperRegistryCenter(zookeeperConfiguration);

    /************************************* for simple job ***********************************/

    // 定义job的配置对象
    private static final JobRootConfiguration simpleJobRootConfig = createSimpleJobRootConfig();

    // 定义任务调度器
    private static final JobScheduler simpleJobScheduler = new JobScheduler(coordinatorRegistryCenter, (LiteJobConfiguration) simpleJobRootConfig, new JobListener());


    /********************************** for dataflow job*************************************/

    private static final LiteJobConfiguration dataFlowJobRootConfig = createDataFlowJobConfiguration();

    private static final JobScheduler dataFlowJobScheduler = new JobScheduler(coordinatorRegistryCenter, dataFlowJobRootConfig, new JobListener());

    public static void main(String[] args) {
        // 初始化注册中心
        coordinatorRegistryCenter.init();
        // 初始化任务调度器
        simpleJobScheduler.init();
        dataFlowJobScheduler.init();
    }

    private static LiteJobConfiguration createSimpleJobRootConfig() {

        // step1 定义核心作业配置对象
        // JavaSimpleJob是job的name, cron表达式， 分片总数
        JobCoreConfiguration simpleJobCoreConfiguration = JobCoreConfiguration.newBuilder("JavaSimpleJob", "0/5 * * * * ?", 10)
//                .jobParameter("none") // job参数
//                .description("this is a JavaSimpleJob") //job描述
//                .failover(true) // 是否故障转移
//                .misfire(true) //遗漏错过时间的人
//                .shardingItemParameters("none") //分片参数？
                .build();

        // 定义simplejob配置对象，这儿依赖核心配置对象，并且配置了具体的job执行类
        SimpleJobConfiguration simpleJobConfiguration = new SimpleJobConfiguration(simpleJobCoreConfiguration, JavaSimpleJob.class.getCanonicalName());

        //  定义Lite作业根配置对象
        LiteJobConfiguration simpleJobRootConfig = LiteJobConfiguration.newBuilder(simpleJobConfiguration).build();

        return simpleJobRootConfig;
    }

    private static LiteJobConfiguration createDataFlowJobConfiguration() {
        JobCoreConfiguration coreConfig = JobCoreConfiguration.newBuilder("myDataFlowTest", "0/10 * * * * ?", 3).shardingItemParameters("0=0,1=1,2=2").build();
        DataflowJobConfiguration dataflowJobConfig = new DataflowJobConfiguration(coreConfig, JavaDataflowJob.class.getCanonicalName(), true);
        LiteJobConfiguration result = LiteJobConfiguration.newBuilder(dataflowJobConfig).build();
        return result;
    }

    private static ZookeeperConfiguration createZookeeperConfiguration() {
        // localhost:2181 是连接的Zookeeper服务器的列表
        // elastic-job-example-jobs是zookeeper下的命名空间，也就是一个根文件夹
        ZookeeperConfiguration zookeeperConfiguration = new ZookeeperConfiguration("localhost:2181", "elastic-job-example-jobs");
        // 等待重试的间隔时间的初始值
        zookeeperConfiguration.setBaseSleepTimeMilliseconds(1000);
        // 等待重试的间隔时间的最大值
        zookeeperConfiguration.setMaxSleepTimeMilliseconds(3000);
        // 最大重试次数
        zookeeperConfiguration.setMaxRetries(3);

        return zookeeperConfiguration;
    }

    public static class JobListener implements ElasticJobListener {

        @Override
        public void beforeJobExecuted(ShardingContexts shardingContexts) {
            logger.info("before java simple job");
        }

        @Override
        public void afterJobExecuted(ShardingContexts shardingContexts) {
            logger.info("after java simple job");
        }
    }

}
