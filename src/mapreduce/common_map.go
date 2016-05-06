package mapreduce

import (
	"io"
	"os"
	"encoding/json"
	"hash/fnv"
	"bufio"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	// TODO:
	// You will need to write this function.
	// You can find the filename for this map task's input to reduce task number
	// r using reduceName(jobName, mapTaskNumber, r). The ihash function (given
	// below doMap) should be used to decide which file a given key belongs into.
	//
	// The intermediate output of a map task is stored in the file
	// system as multiple files whose name indicates which map task produced
	// them, as well as which reduce task they are for. Coming up with a
	// scheme for how to store the key/value pairs on disk can be tricky,
	// especially when taking into account that both keys and values could
	// contain newlines, quotes, and any other character you can think of.
	//
	// One format often used for serializing data to a byte stream that the
	// other end can correctly reconstruct is JSON. You are not required to
	// use JSON, but as the output of the reduce tasks *must* be JSON,
	// familiarizing yourself with it here may prove useful. You can write
	// out a data structure as a JSON string to a file using the commented
	// code below. The corresponding decoding functions can be found in
	// common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!
	f, err := os.Open(inFile)
    if err != nil {
   	   return
    }	
    defer f.Close()

    reduce_fd_map := make(map[string] *json.Encoder, nReduce)
    bfRd := bufio.NewReader(f)
    for {
        line, err := bfRd.ReadBytes('\n')
   		kv_list := mapF(inFile, string(line))
   		for _, kv := range kv_list {
   			reduceTaskNumber := int(ihash(kv.Key)) % nReduce
   			reduce_fname := reduceName(jobName, mapTaskNumber, reduceTaskNumber)
   			enc, ok := reduce_fd_map[reduce_fname]
   			if !ok {
   				fd, err := os.Create(reduce_fname)
   				if err != nil {
   					return
   				}
   				enc = json.NewEncoder(fd)
   				reduce_fd_map[reduce_fname] = enc 
   				defer fd.Close()
   			}
   			err = enc.Encode(&kv)
   			if err != nil {
   				return
   			}
   		}
   		// fixme
        if err != nil { //遇到任何错误立即返回，并忽略 EOF 错误信息
            if err == io.EOF {
              return
            }
        return
        }
    }
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
