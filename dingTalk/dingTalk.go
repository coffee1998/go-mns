package dingTalk

import (
	"fmt"
	"github.com/coffee1998/go-mns/config"
	"github.com/coffee1998/go-mns/util/curl"
	"log"
)

//发送钉钉消息到个人
func SendMessageToUser(config config.DingTalkConfig) func() error {
	return func() error {
		if config.Url == "" {
			log.Fatal("url is required")
		}
		if config.UserList == "" {
			log.Fatal("UserList is required")
		}
		postData := map[string]string{
			"useridList":	config.UserList,
			"message": 		config.Message,
		}
		headers := map[string]string{
			"Content-Type": "application/x-www-form-urlencoded",
		}
		req := curl.NewRequest()
		req.SetDialTimeOut(30)
		resp, err := req.SetUrl(config.Url).SetHeaders(headers).SetFormData(postData).Post()
		if err != nil {
			fmt.Println("发送失败，失败内容："+resp.Body)
			return err
		}
		return nil
	}
}

//发送钉钉消息到群组
func SendMessageToGroup(config config.DingTalkConfig) func() error {
	return func() error {
		if config.Url == "" {
			log.Fatal("url is required")
		}
		if config.GroupId == "" {
			log.Fatal("GroupId is required")
		}
		postData := map[string]string{
			"_id":     config.GroupId,
			"message": config.Message,
		}
		headers := map[string]string{
			"Content-Type": "application/x-www-form-urlencoded",
		}
		req := curl.NewRequest()
		req.SetDialTimeOut(30)
		resp, err := req.SetUrl(config.Url).SetHeaders(headers).SetFormData(postData).Post()
		if err != nil {
			fmt.Println("发送失败，失败内容："+resp.Body)
			return err
		}
		return nil
	}
}
