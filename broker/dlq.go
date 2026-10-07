package broker

import "sync"

// DLQ 是按到达顺序保存死信的内存队列。
// 同一条消息只会保留一条死信记录（用 messageID 去重），
// 重试/重投不会在死信队列里产生重复条目。
type DLQ struct {
	mu      sync.Mutex
	items   []*DeadLetter
	byMsgID map[int64]*DeadLetter
}

func newDLQ() *DLQ {
	return &DLQ{byMsgID: make(map[int64]*DeadLetter)}
}

// add 放入一条死信；若该消息已在死信队列（正常不会发生），保留第一次的记录。
func (d *DLQ) add(dl *DeadLetter) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.byMsgID[dl._messageID]; ok {
		return
	}
	d.byMsgID[dl._messageID] = dl
	d.items = append(d.items, dl)
}

// List 返回死信快照，可按 topic/group 过滤（传空字符串表示不过滤）。
func (d *DLQ) List(topic, group string) []*DeadLetter {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]*DeadLetter, 0, len(d.items))
	for _, dl := range d.items {
		if topic != "" && dl.Topic != topic {
			continue
		}
		if group != "" && dl.Group != group {
			continue
		}
		cp := *dl
		out = append(out, &cp)
	}
	return out
}

// remove 取出并删除指定消息 ID 的死信，不存在返回 nil。
func (d *DLQ) remove(messageID int64) *DeadLetter {
	d.mu.Lock()
	defer d.mu.Unlock()
	dl, ok := d.byMsgID[messageID]
	if !ok {
		return nil
	}
	delete(d.byMsgID, messageID)
	for i, it := range d.items {
		if it._messageID == messageID {
			d.items = append(d.items[:i], d.items[i+1:]...)
			break
		}
	}
	return dl
}

// Len 返回死信总数。
func (d *DLQ) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.items)
}
