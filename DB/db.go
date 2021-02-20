package DB

import (
	"github.com/globalsign/mgo"
)

const (
	DataBase = "coffee1998_mns"
	RetryC   = "retry"
)

func Exec(mongoURI string, job func(*mgo.Collection) error) error  {
	var session *mgo.Session
	var err error

	if mongoURI != "" {
		if session, err = mgo.Dial(mongoURI); err == nil {
			//log.Fatal("Failed to connection a MongoDB")
			session.SetSafe(&mgo.Safe{})
			collection := session.DB(DataBase).C(RetryC)
			err = job(collection)
			session.Close()
		}
	}
	return err
}
