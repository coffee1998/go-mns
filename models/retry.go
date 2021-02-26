package models

type Retry struct {
	Id         int    `json:"id" comment:"id"`
	RetryNum   int    `json:"retry_num" comment:"重试次数"`
	IsDone     int    `json:"is_done" comment:"是否重试完毕 0：否 1：是"`
	IsSucc     int    `json:"is_succ" comment:"是否发送队列成功 0：否 1：是"`
	ErrMsg     string `json:"err_msg" comment:"异常信息"`
	Data       string `json:"data" comment:"入列数据"`
	TopicName  string `json:"topic_name" comment:"主题名称"`
	UpdateTime int64  `json:"update_time" comment:"更新时间"`
}
