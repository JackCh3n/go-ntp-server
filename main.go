package main

import (
	"encoding/binary"
	"log"
	"net"
	"os"
	"time"
)

const (
	ntpEpochOffset = 2208988800 // 1900-1970秒数差
	port           = ":123"
	logFileName    = "ntp_access.log"
)

var logger *log.Logger

func initLogger() {
	f, err := os.OpenFile(logFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("failed to open log file: %v", err)
	}
	logger = log.New(f, "", log.LstdFlags)
}

func main() {
	initLogger()

	addr, err := net.ResolveUDPAddr("udp", port)
	if err != nil {
		log.Fatalf("failed to resolve UDP addr: %v", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	defer conn.Close()

	log.Printf("NTP server listening on %s", port)

	for {
		buf := make([]byte, 48)
		n, clientAddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			log.Printf("error reading: %v", err)
			continue
		}
		if n < 48 {
			log.Printf("received short packet from %v", clientAddr)
			continue
		}
		go handleNTPRequest(conn, clientAddr, buf)
	}
}

func handleNTPRequest(conn *net.UDPConn, addr *net.UDPAddr, req []byte) {
	// 创建响应包
	resp := make([]byte, 48)
	
	// 设置NTP头部: LI=0, VN=4, Mode=4 (服务器模式)
	resp[0] = 0x24 // 00100100: LI=0, VN=4, Mode=4
	
	// 设置有效的Stratum层级 (1=一级服务器)
	resp[1] = 1 // Stratum: 1 (一级服务器)
	
	// 设置其他必要字段
	resp[2] = 0x0A // Poll: 10 (1024秒轮询间隔)
	resp[3] = 0xEC // Precision: -20 (约微秒级精度)
	
	// 根延迟和根离散设为0
	binary.BigEndian.PutUint32(resp[4:8], 0)
	binary.BigEndian.PutUint32(resp[8:12], 0)
	
	// 参考ID设为本地时钟标识
	copy(resp[12:16], []byte{'L', 'O', 'C', 'L'}) // "LOCL"
	
	// 获取当前时间
	now := time.Now().UTC()
	
	// 解析请求中的原始时间戳(T1)
	t1Sec := binary.BigEndian.Uint32(req[40:44])
	t1Frac := binary.BigEndian.Uint32(req[44:48])
	
	// 计算服务器接收时间(T2) - 必须在T1之后
	t2 := now
	if t2.UnixNano() <= int64(t1Sec-ntpEpochOffset)*1e9+int64(float64(t1Frac)*(1e9/(1<<32))) {
		// 如果服务器时间早于T1，人为增加一个微小偏移
		t2 = t2.Add(100 * time.Millisecond)
	}
	
	// 计算服务器发送时间(T3) - 必须在T2之后
	t3 := t2.Add(10 * time.Millisecond)
	
	// 设置时间戳
	setNTPTime(resp[16:24], time.Unix(0, 0)) // 参考时间戳 (设为0)
	setNTPTime(resp[24:32], parseNTPTime(req[40:48])) // 原始时间戳 (T1)
	setNTPTime(resp[32:40], t2) // 接收时间戳 (T2)
	setNTPTime(resp[40:48], t3) // 传输时间戳 (T3)

	_, err := conn.WriteToUDP(resp, addr)
	if err != nil {
		log.Printf("error sending response to %v: %v", addr, err)
	}

	go logRequest(addr)
}

// 设置NTP时间戳 (64位: 32位秒 + 32位小数)
func setNTPTime(b []byte, t time.Time) {
	secs := uint32(t.Unix() + ntpEpochOffset)
	frac := uint32((uint64(t.Nanosecond()) << 32) / 1e9)
	binary.BigEndian.PutUint32(b[0:4], secs)
	binary.BigEndian.PutUint32(b[4:8], frac)
}

// 解析NTP时间戳
func parseNTPTime(b []byte) time.Time {
	secs := int64(binary.BigEndian.Uint32(b[0:4])) - ntpEpochOffset
	frac := float64(binary.BigEndian.Uint32(b[4:8])) / (1 << 32)
	nsec := int64(frac * 1e9)
	return time.Unix(secs, nsec).UTC()
}

func logRequest(addr *net.UDPAddr) {
	ip := addr.IP.String()
	names, err := net.LookupAddr(ip)
	hostname := "-"
	if err == nil && len(names) > 0 {
		hostname = names[0]
	}
	now := time.Now().Format("2006-01-02 15:04:05")
	logger.Printf("Hora consultada em %s por %s (hostname: %s)\n", now, ip, hostname)
}