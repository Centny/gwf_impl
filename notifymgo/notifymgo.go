package notifymgo

import (
	"github.com/Centny/gwf/netw/rc/plugin"
	"github.com/Centny/gwf/util"
	"gopkg.in/bson.v2"
	"gopkg.in/mongoc.v1"
)

//Indexes is the mongo findex field
var Indexes = []*mongoc.Index{
	{Name: "message_oid", Key: []string{"oid"}},
	{Name: "message_owner", Key: []string{"owner"}},
	{Name: "message_type", Key: []string{"type"}},
	{Name: "message_marked", Key: []string{"marked"}},
	{Name: "message_count", Key: []string{"count"}},
	{Name: "message_time", Key: []string{"time"}},
}

//NotifyMgo is impl to NotifyDb by mongo
type NotifyMgo struct {
	Name  string
	C     func(name string) *mongoc.Collection
	Count map[string]int
}

//NewNotifyMgo is NotifyMgo creator
func NewNotifyMgo(c func(name string) *mongoc.Collection) *NotifyMgo {
	return &NotifyMgo{
		Name:  "rc_notify",
		C:     c,
		Count: map[string]int{},
	}
}

//AddMessage @see NotifyDb
func (n *NotifyMgo) AddMessage(m *plugin.Message) error {
	m.ID = bson.NewObjectId().Hex()
	m.Time = util.Now()
	return n.C(n.Name).Insert(m)
}

//RemoveMessage @see NotifyDb
func (n *NotifyMgo) RemoveMessage(id string) error {
	_, err := n.C(n.Name).Remove(bson.M{"_id": id}, true)
	return err
}

//DoneMessage @see NotifyDb
func (n *NotifyMgo) DoneMessage(mid, key string) (msg *plugin.Message, err error) {
	msg = &plugin.Message{}
	_, err = n.C(n.Name).FindAndModify(
		bson.M{
			"_id": mid,
			"marked": bson.M{
				"$ne": key,
			},
		}, bson.M{
			"$addToSet": bson.M{
				"marked": key,
			},
			"$inc": bson.M{
				"count": 1,
			},
		}, nil, false, true, msg)
	return
}

//RemoveCount @see NotifyDb
func (n *NotifyMgo) RemoveCount(mtype string) (count int, err error) {
	if val, ok := n.Count[mtype]; ok {
		return val, nil
	}
	return 1, nil
}

//ListMessage @see NotifyDb
func (n *NotifyMgo) ListMessage(m *plugin.Message) (ms []*plugin.Message, err error) {
	err = n.C(n.Name).Find(bson.M{"type": m.Type}, nil, 0, 0, &ms)
	return
}
