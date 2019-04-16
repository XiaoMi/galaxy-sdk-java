# Talos stress test

galaxy.talos.service.endpoint=
galaxy.talos.accessKey=
galaxy.talos.accessSecret=
galaxy.talos.topicName=
// set to NONE for exactly stress messagebytein
galaxy.talos.producer.compression.type=NONE
// set partitionid which need stress, default all partition
galaxy.talos.partitionid=
// putthread number for every partiton, which control concurrency
thread.num.perpartition=
// qps for single putthread, which must be divided evenly by 1000. Not execatly control when lantency is less than 1000/qps.perthread. Default is as fast as possible
qps.perthread=200
// set message, default is a 1k message
message.str="message body"

* case 1: wanna stress put qps ?
exp: 2000 qps for perpartition
thread.num.perpartition=20 // 20 concurrency putRequest
qps.perthread=100 // 100 qps perthread, cause latency is less than 10ms
       if you want more qps for one single partition, start one more client and set galaxy.talos.partitionid

* case 2: wanna stress put messagebytein/s ?
exp: 1M/s for perpartition
thread.num.perpartition=10 // 10 concurrency putRequest
qps.perthread=100 // 100 qps perthread, cause latency is less than 10ms
message.str= // default 1k
galaxy.talos.producer.compression.type=NONE

* case 3: wanna stress throughput ？
exp: 
client num && thread.num.perpartition // total thread number for one partition need to more than 100
qps.perthread= // do not set, for fastest qps!
message.str=  // set a enough large message; set to default could stress qps limit
galaxy.talos.producer.compression.type=NONE
