package DB

import (
	"database/sql"
	"github.com/coffee1998/go-mns/models"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"time"
)

const (
	DataBase   = "oam_mns.db"
	RetryTable = "retry"
)

func init() {
	CreateTable()
}

func GetDB() (*sql.DB, error) {
	return sql.Open("sqlite3", DataBase)
}

func CreateTable() error {
	db, err := GetDB()
	if err != nil {
		return err
	}
	defer db.Close()

	log.Println("Create student table...")
	sql := `CREATE TABLE IF NOT EXISTS ` + RetryTable + ` (
		"id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
		"retry_num" INTEGER NOT NULL DEFAULT 0,
		"is_done" INTEGER NOT NULL DEFAULT 0,
		"is_succ" INTEGER NOT NULL DEFAULT 0,
		"err_msg" TEXT NOT NULL DEFAULT '',
		"data" TEXT NOT NULL DEFAULT '',
		"topic_name" TEXT NOT NULL DEFAULT '',
		"update_time" INTEGER NOT NULL DEFAULT 0
	)`
	_, err = db.Exec(sql)
	return err
}

func InsertRetry(errMsg, data, topicName string, updateTime int64) error {
	db, err := GetDB()
	if err != nil {
		return err
	}
	defer db.Close()

	insertSQL := `INSERT INTO ` + RetryTable + `(err_msg, data, topic_name, update_time) VALUES (?, ?, ?, ?)`
	statement, err := db.Prepare(insertSQL)
	if err != nil {
		return err
	}
	_, err = statement.Exec(errMsg, data, topicName, updateTime)
	if err != nil {
		return err
	}
	return nil
}

func UpdateRetry(retryNum, isDone int, errMsg string, id int) error {
	db, err := GetDB()
	if err != nil {
		return err
	}
	defer db.Close()

	updateSQL := `UPDATE ` + RetryTable + ` SET retry_num=?, is_done=?, update_time=?, err_msg=? WHERE id=?`
	stmt, err := db.Prepare(updateSQL)
	if err != nil {
		return err
	}
	_, err = stmt.Exec(retryNum, isDone, time.Now().Unix(), errMsg, id)
	if err != nil {
		return err
	}
	return nil
}

func DelByID(id int) error {
	db, err := GetDB()
	if err != nil {
		return err
	}
	defer db.Close()

	sql := `DELETE FROM ` + RetryTable + ` WHERE id=?`
	stmt, err := db.Prepare(sql)
	if err != nil {
		return err
	}
	_, err = stmt.Exec(id)
	if err != nil {
		return err
	}
	return nil
}

func QueryRetry(retryTimes int, topicName string, limit int, page int) ([]*models.Retry, error) {
	db, err := GetDB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	sql := `SELECT id, retry_num, is_done, is_succ, data, topic_name, update_time FROM ` + RetryTable + ` WHERE retry_num<? AND is_done=0 AND topic_name=? LIMIT ? OFFSET ?`
	stmt, err := db.Prepare(sql)
	if err != nil {
		return nil, err
	}
	rows, err := stmt.Query(retryTimes, topicName, limit, (page-1)*limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var (
		retryNum, isDone, isSucc, id int
		data, topic                  string
		updateTime                   int64
	)
	var ret = make([]*models.Retry, 0)
	for rows.Next() {
		rows.Scan(&id, &retryNum, &isDone, &isSucc, &data, &topic, &updateTime)
		ret = append(ret, &models.Retry{Id: id, RetryNum: retryNum, IsDone: isDone, IsSucc: isSucc, Data: data, TopicName: topic, UpdateTime: updateTime})
	}
	return ret, nil
}
