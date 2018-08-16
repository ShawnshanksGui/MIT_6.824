package mapreduce


import (
	"os"
	"fmt"
	"sort"
	"io/ioutil"
	"encoding/json"
//	"strconv"
	"strings"
)

type By func(a, b *KeyValue) bool

func (by By) Sort(key_values []KeyValue) {
	kvs := &KeyValue_sort{
		key_values: key_values,
		by:         by,
	}
	sort.Sort(kvs)
}

type KeyValue_sort struct{
	key_values []KeyValue
	by func(a, b *KeyValue) bool
}

//build sort.Interface
func (s *KeyValue_sort) Len() int {
	return len(s.key_values)
}

func (s *KeyValue_sort) Swap(i, j int) {
	s.key_values[i], s.key_values[j] = s.key_values[j], s.key_values[i]
}

func(s *KeyValue_sort) Less(i, j int) bool {
	return s.by(&s.key_values[i], &s.key_values[j]) 
}


func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).

	//***written by Shawnshanks starting in 20180813.pm
	// finishing in 20180815.pm 	
	//==================================================================

	var key_value_sum []KeyValue

	for i := 0; i < nMap; i++ {
		filename_input := reduceName(jobName, i, reduceTask)

		f_input, err := os.Open(filename_input)
		if err != nil {fmt.Println("error", err)}
	
		/*
		byteValue, _ := ioutil.ReadAll(f_input)

		fmt.Printf("The lenght of byteValue is %d\n", len(byteValue))
		fmt.Printf("%s", byteValue)

		var key_values []KeyValue
		err = json.Unmarshal(byteValue, &key_values)
		if err != nil {
			fmt.Println("error:", err)
		}

		fmt.Printf("the length of key_values is %d\n", len(key_value_sum))
		fmt.Println(key_values)

		for _, element := range key_values {
			key_value_sum = append(key_value_sum, element)
		}
		*/		

		byteValue, _ := ioutil.ReadAll(f_input)
		dec := json.NewDecoder(strings.NewReader(string(byteValue)))
//		t, err := dec.Token()
//		if err != nil {
//			fmt.Println("error", err)
//		}		
		for dec.More() {
			var kv KeyValue
			err := dec.Decode(&kv)   //why it does not work when using unmarshal? 
			if(err != nil) {
				println("error", err)
			}
			key_value_sum = append(key_value_sum, kv)
		}
/*
		var kv KeyValue
		err = json.Unmarshal(byteValue, &key_values)
		if err != nil {
			fmt.Println("error:", err)
		}

		scanner := bufio.NewScanner(f_input)
		for scanner.Scan() {
			str_kv = scanner.Text()

		}
*/
		f_input.Close()
	}

// sort 
/*	
	sort.Slice(key_value_sum, func(i, j int) bool {
	//	return strconv.Atoi(key_value_sum[i].Key)<strconv.Atoi(key_value_sum[j].Key)
		i_int, err := strconv.Atoi(key_value_sum[i].Key)
		if err != nil {fmt.Println("error", err)}
		j_int, err := strconv.Atoi(key_value_sum[j].Key)
		if err != nil {fmt.Println("error", err)}

		return i_int < j_int
	})
*/
//	sort.Slice(key_value_sum, func(a, b string) bool {
//		return strings.Compare(a, b)
//	})


	key_order := func(a, b *KeyValue) bool {
		return a.Key < b.Key
	}

	By(key_order).Sort(key_value_sum)
//	sort.Strings(key_value_sum)

	f_output, err := os.Create(outFile)
	if err != nil {fmt.Println("error", err)}
	

//========
	var key_prev string 
	var value_kvs_tmp []string

	var cnt int = 0
	enc := json.NewEncoder(f_output)

	for _, kv := range key_value_sum {
		if cnt == 0 {
			cnt = 1

			key_prev = kv.Key //
			value_kvs_tmp = append(value_kvs_tmp, kv.Value)

			continue
		}

		if key_prev == kv.Key {
			value_kvs_tmp = append(value_kvs_tmp, kv.Value)
		} else {
		// You should call it once per distinct key, with a slice of all the values
		// for that key. reduceF() returns the reduced value for that key.			
			enc.Encode(&(KeyValue{key_prev, reduceF(key_prev, value_kvs_tmp)}))
			
			key_prev = kv.Key
			value_kvs_tmp = []string{} 
			value_kvs_tmp = append(value_kvs_tmp, kv.Value)
		}
	}
	enc.Encode(&(KeyValue{key_prev, reduceF(key_prev, value_kvs_tmp)}))
//========

	f_output.Close()
	//
}