package config

const DefaultRetryTimes = 5
const DefaultRetrySecond = 120
const GoroutineCount = 100

type NoticeFunc func() error

//:param Endpoint: 必填
//:param AccessKeyID: 必填
//:param AccessKeySecret: 必填
//:param TopicName: 队列主题名称，必填
//:param RetryIntervalInSecond: 重试定时时间，非必填
//:param RetryIntervalInTimes: 重试次数，非必填

type MNSConfig struct {
	Endpoint              string
	AccessKeyId           string
	AccessKeySecret       string
	TopicName             string
	RetryIntervalInSecond int
	RetryIntervalInTimes  int
}

//:param Url: 发送钉钉消息的url地址，这里使用光环助手服务，必填
//:param UserList: 要发送到某个人钉钉的userid，多个用,分隔
//:param GroupId: 要发送到钉钉群的群id
//:param Message: 要发送的钉钉消息
type DingTalkConfig struct {
	Url      string
	UserList string
	GroupId  string
	Message  string
}
