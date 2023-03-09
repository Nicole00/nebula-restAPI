package com.vesoft.nebula.graph.server.entity;

public enum ErrorCode {

    /**
     * 通用信息
     */

    ERROR(-1, "失败"),

    SUCCESS(0, "成功"),

    COMMAND_FILE_NOT_FOUND(2, "没有文件或目录"),

    EMPTY_USER(3, "user 信息为空"),

    EMPTY_JOB_NAME(4, "任务名为空"),

    INTERNAL_ERROR(5, "其他错误，请前往日志查看详情"),

    /**
     * io error code, start with 0
     */
    CONFIG_FILE_WRITE_ERROR(9, "配置文件读写失败"),

    /**
     * Nebula error code,  start with 1 and 2
     */
    LACK_NEBULA_CONFIG(10, "缺少Nebula配置"),

    INVALID_CONNECTION_PARAMETER(11, "连接参数无效"),

    INVALID_META_ADDRESS(12, "Meta服务地址无效"),

    INVALID_GRAPH_ADDRESS(13, "Graph服务地址无效"),

    INVALID_SPACE_LABEL(14, "Nebula的Space或Label为空"),

    // special error message for web
    CONNECT_ERROR(15, "broken pipe"),

    NOT_CONNECT(16, "connection refused for lack of session"),

    INVALID_CONNECT(17, "an existing connection was forcibly closed"),

    EMPTY_NGQL(18, "NGQL为空"),

    EXECUTE_ERROR(19, "Nebula NGQL 执行失败"),

    ENCODE_ERROR(20, "Nebula 结果编码异常"),

    ALGO_RESULT_WRITE_NEBULA_ERROR(21, "算法结果写 Nebula 失败"),

    SPACE_NOT_EXIST(22, "Space 在Nebula中不存在"),

    TAG_NOT_EXIST(23, "tag 在Nebula中不存在"),

    EDGE_NOT_EXIST(24, "edge 在Nebula中不存在"),

    WEIGHT_COL_NOT_EXIST(25, "weight col在Nebula中不存在"),

    NEBULA_COL_NOT_EXIST(26, "属性名algoCols在Nebula中不存在"),



    /**
     * hdfs error code, start with 3 and 4
     */

    HDFS_FORMAT_ERROR(30, "hdfs 格式异常，必须是hdfs://NAMENODE:port/"),

    FILE_PATH_ERROR(31, "路径格式异常，必须以/开头"),

    LACK_FILE_CONFIG(32, "缺少文件配置"),

    PATH_NOT_EXIST(33, "路径不存在"),

    WRITE_HDFS_FILE_ERROR(34, "HDFS写失败，请检查权限"),

    PATH_CANNOT_ACCESSED(35, "路径无法访问，请检查hdfs路径和权限"),

    PATH_ALREADY_EXIST(36, "结果路径已存在"),

    INVALID_HDFS_NAMENODE(37, "图计算中 hdfs 前缀参数不一致"),

    INVALID_FILE_CONFIG(38, "文件路径和分隔符配置错误"),

    INVALID_FILE_HEADER(39, "文件表头配置错误"),

    COMMUNITY_REPORT_WRITE_CSV_ERROR(40, "社群报告结果写CSV失败"),


    /**
     * HIVE error code, start with 5
     */
    LACK_HIVE_CONFIG(50, "缺少HIVE配置"),

    SPARK_SQL_EXECUTE_ERROR(51, "SparkSQL 执行失败"),

    WRONG_SQL_FORMAT(52, "SparkSQL 格式异常，参考 select src,dst from db.table"),

    SPARK_ENABLE_HIVE_ERROR(53, "Spark连接HIVE失败"),

    INVALID_HIVE_CONNECTION(54, "Hive连接配置为空"),

    INVALID_TABLE_NAME(55, "Hive Table不合法"),

    TABLE_NOT_EXIST(56, "Hive Table不存在"),

    ALGO_RESULT_WRITE_HIVE_ERROR(57, "结果写HIVE表失败"),

    HIVE_SQL_EXECUTE_ERROR(58, "Hive SQL 执行失败"),


    /**
     * Algo error code, start with 6 and 7
     */

    NOT_SUPPORT_ALGO(60, "算法不支持"),

    LACK_LOUVAIN_CONFIG(61, "缺少Louvain的配置"),

    LACK_KCORE_CONFIG(62, "缺少KCore的配置"),

    LACK_LPA_CONFIG(63, "缺少Lpa的配置"),

    LACK_HANP_CONFIG(64, "缺少Hanp的配置"),

    LACK_PAGERANK_CONFIG(65, "缺少PageRank的配置"),

    LACK_CC_CONFIG(66, "缺少CC的配置"),

    LACK_CUSTOMED_CONFIG(67, "缺少自定义算法的配置"),

    INVALID_LOUVAIN_CONFIG(68, "Louvain配置无效"),

    INVALID_KCORE_CONFIG(69, "KCore配置无效"),

    INVALID_LPA_CONFIG(70, "LPA配置无效"),

    INVALID_HANP_CONFIG(71, "HANP配置无效"),

    INVALID_CC_CONFIG(72, "CC配置无效"),

    INVALID_PAGERANK_CONFIG(73, "PageRank配置无效"),

    ALGO_PARAMETER_FORMAT_ERROR(74, "数据源参数格式异常，json转换失败"),

    ALGO_NEED_ENCODE_ERROR(75, "是否需要编码的配置不一致"),




    /**
     * spark error code, start with 8
     */
    ERROR_SPARK_COMMAND(80, "执行命令异常,请检查参数信息"),

    INVALID_PARAMETER_JSON(81, "任务参数json格式错误"),

    SPARK_SESSION_ERROR(82, "获取SparkSession失败，请检查spark服务或服务端配置项--master"),

    VERTEX_ID_ENCODE_ERROR(83, "Vertex id 编码失败"),

    VERTEX_ID_DECODE_ERROR(84, "Vertex id 解码失败"),

    SAVE_ENCODE_DATA_ERROR(85, "Vertex id 存HDFS失败，检查hdfs执行权限"),

    READE_ENCODED_DATA_ERROR(86, "读取编码数据或算法结果失败"),

    NULL_ENCODED_DATA(87, "编码结果为空，检查算法结果是否为空"),

    COMPUTE_COMMUNIT_REPORT_ERROR(88, "社群报告统计计算失败"),


    /**
     * reader error code, start with 10
     */
    UNKNOWN_READER_ERROR(100, "数据源读取发生未知异常"),

    LACK_DATA_SOURCE(101, "缺少数据源配置"),

    UNSUPPORT_DATA_SOURCE(102, "数据源不支持"),

    READE_PARAMETER_FORMAT_ERROR(103, "数据源参数格式异常，json转换失败"),

    READ_CSV_DATA_ERROR(104, "读取CSV数据失败"),

    READ_HIVE_DATA_ERROR(105, "读取HIVE数据失败"),

    READ_NEBULA_DATA_ERROR(106, "读取Nebula数据失败"),

    SELECT_FROM_SOURCE_DATA_ERROR(107, "从源数据获取源点、 目标点、权重列失败，请检查列名参数是否在源数据中存在"),

    READ_TASK_ERROR(108, "读取源数据任务提交异常"),

    WRONG_WEIGH_COL_SETTING(109, "权重列设置错误，仅支持所有源数据都设置或都不设置"),

    /**
     * writer error code, start with 11
     */
    UNKNOWN_WRITER_ERROR(110, "算法结果保存发生未知异常"),

    UNSUPPORT_DATA_SINK(111, "存储结果池不支持"),

    WRITER_PARAMETER_FORMAT_ERROR(112, "数据源参数格式异常，json转换失败"),

    ALGO_RESULT_WRITE_ERROR(113, "结果存储文件失败"),

    UNSUPPORT_WRITE_MODE(114, "写入模式不支持,可选项为insert和update"),

    WRITE_TASK_ERROR(115, "图算法结果写入任务提交异常"),


    /**
     * plato code, start with 12
     */
    UNKNOWN_PLATO_ERROR(120, "提交run_plato命令异常"),

    FILE_NOT_FOUND(121, "plato执行文件未找到"),

    ALGO_PARAMETER_NOT_SET(122, "算法参数缺失"),

    HDFS_NOT_SET(123, "hdfs未指定"),

    COMPUTE_TASK_ERROR(124, "图算法执行任务提交异常"),

    ID_NOT_ENCODE(126, "数据未编码"),



    /**
     * community report code, start with 13 and 14
     */
    LACK_REPORT_CONFIG(130, "缺少社群报告参数"),

    REPORT_PARAMETER_FORMAT_ERROR(131, "报告统计参数格式异常，json转换失败"),

    REPORT_TASK_ERROR(132, "社群报告统计任务提交异常"),

    LACK_RESULT_PATH(133, "结果存储路径为空"),

    LACK_VERTEX_SOURCE_CONFIG(134, "点源数据配置为空"),

    LACK_ALGO_SOURCE_CONFIG(135, "算法结果数据配置为空"),

    EMPTY_CSV_PATH(136, "csv源数据的path配置为空"),

    ERROR_CSV_HEADER(137, "csv无表头，则列名必须以'_c'开头"),

    EMPTY_HIVE_SQL(138, "Hive源数据的sql配置为空"),

    EMPTY_NEBULA_CONFIG(139, "Nebula配置为空，请检查meta地址、space、label"),

    READ_CSV_VERTEX_ERROR(140, "读取CSV点数据源失败"),
    READ_CSV_ALGORITHM_ERROR(141, "读取CSV算法结果数据源失败"),

    READ_HIVE_VERTEX_ERROR(142, "读取HIVE点数据源失败"),
    READ_HIVE_ALGORITHM_ERROR(143, "读取HIVE算法结果数据源失败"),

    READ_NEBULA_VERTEX_ERROR(144, "读取Nebula点数据源失败"),
    READ_NEBULA_ALGORITHM_ERROR(145, "读取Nebula算法结果数据源失败"),
    ;

    private final int errorCode;
    private final String errorMsg;

    ErrorCode(int errorCode, String errorMsg) {
        this.errorCode = errorCode;
        this.errorMsg = errorMsg;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public String getErrorMsg() {
        return errorMsg;
    }


    /**
     * get ErrorCode according to the code
     */
    public static ErrorCode getErrorCode(int code) {
        for(ErrorCode errorCode: ErrorCode.values()){
            if(code == errorCode.getErrorCode()){
                return errorCode;
            }
        }
        return INTERNAL_ERROR;
    }
}
