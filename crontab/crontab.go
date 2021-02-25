package crontab

import (
	"fmt"
	"github.com/coffee1998/go-mns/config"
	"github.com/robfig/cron"
)

type FuncJob func()
var FuncJobs map[string]FuncJob = make(map[string]FuncJob)
var Cron *cron.Cron = cron.New()

func AddJob(spec int, key string, funcJob FuncJob)  {
	if spec == 0 {
		spec = config.DefaultRetrySecond
	}
	if _, ok := FuncJobs[key]; ok {

	} else {
		Cron.AddFunc(fmt.Sprintf("*/%d * * * * *", spec), funcJob)
		FuncJobs[key] = funcJob
	}
	Cron.Start()
}