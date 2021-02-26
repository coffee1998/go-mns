package DB

import (
	"errors"
	"github.com/globalsign/mgo"
)

const (
	DataBase = "oam_mns"
	RetryC   = "retry"
)

func GetMongoSession(mongoURI string) (*mgo.Session, error) {
	if mongoURI == "" {
		return nil, errors.New("mongoURI is empty")
	}

	var session *mgo.Session
	var err error
	if session, err = mgo.Dial(mongoURI); err == nil{
		session.SetSafe(&mgo.Safe{})
		return session, nil
	} else {
		return nil, err
	}
}

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
