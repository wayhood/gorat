package gorat

import (
	rds "github.com/garyburd/redigo/redis"
	"time"
	"fmt"
	"encoding/json"
	"reflect"
	"strings"
)

var typeRegistry = make(map[string]reflect.Type, 0)


type WorkerInterface interface {
	Run()
}


type Worker struct {
	run   WorkerInterface
	redis rds.Conn
	queue string
	Retry bool
}

type Payload struct {
	Retry bool
	Queue string
	Class string
	Args string
	JobId int64
	EnqueuedAt int64
}

//注册Woker类型
func RegisterWorker(t reflect.Type) {
	typeRegistry[t.String()] = t
}

//创建Worker
func CreateWorker(r WorkerInterface, queueName string, redis rds.Conn) Worker {
	w := Worker{
		run:r,
		redis:redis,
		queue:queueName,
	}
	return w
}

func (w *Worker) GetRandomId() int64 {
	return time.Now().Unix()
}

func (w *Worker) Run() {
	fmt.Println("Retry:", w.Retry)
	fmt.Println("Queue:", w.queue)
	//反射类型
	//fmt.Println()
	w.run.Run()
}

func (w *Worker) RunAsync() {
	if w.queue == "" {
		w.queue = "default"
	}

	args, err := json.Marshal(w.run)
	payload := Payload{
		Retry:      w.Retry,
		Queue:      w.queue,
		Class:      reflect.TypeOf(w.run).Elem().String(),
		Args:       string(args),
		JobId:      w.GetRandomId(),
		EnqueuedAt: time.Now().UnixNano(),
	}

	//b, err := json.MarshalIndent(payload, "", "    ")
	b, err := json.Marshal(payload)
	if err == nil {
		w.redis.Do("LPUSH", w.queue, string(b))
	}
}

//根据类型创建一个Worker反射实例
func newWorkerInstance(typeName string) (reflect.Value, bool) {
	elem, ok := typeRegistry[typeName]
	if !ok {
		return reflect.Value{}, false
	}
	//reflect.New(elem).Elem().Interface()
	return reflect.New(elem), true

}

func invokeValue(vType reflect.Type, v reflect.Value) reflect.Value {
	var ret reflect.Value
	switch(vType.Kind()) {
	case reflect.Bool:
		ret = reflect.ValueOf(v.Interface().(bool))
	case reflect.Int:
		ret = reflect.ValueOf(int(v.Interface().(float64)))
	case reflect.Int8:
		ret = reflect.ValueOf(int8(v.Interface().(float64)))
	case reflect.Int16:
		ret = reflect.ValueOf(int16(v.Interface().(float64)))
	case reflect.Int32:
		ret = reflect.ValueOf(int32(v.Interface().(float64)))
	case reflect.Int64:
		ret = reflect.ValueOf(int64(v.Interface().(float64)))
	case reflect.Uint:
		ret = reflect.ValueOf(uint(v.Interface().(float64)))
	case reflect.Uint8:
		ret = reflect.ValueOf(uint8(v.Interface().(float64)))
	case reflect.Uint16:
		ret = reflect.ValueOf(uint16(v.Interface().(float64)))
	case reflect.Uint32:
		ret = reflect.ValueOf(uint32(v.Interface().(float64)))
	case reflect.Uint64:
		ret = reflect.ValueOf(uint64(v.Interface().(float64)))
	case reflect.Uintptr:
		ret = reflect.ValueOf(uintptr(v.Interface().(float64)))
	case reflect.Float32:
		ret = reflect.ValueOf(float32(v.Interface().(float64)))
	case reflect.Float64:
		ret = reflect.ValueOf(v.Interface().(float64))
	case reflect.String:
		ret = reflect.ValueOf(v.Interface().(string))
	case reflect.Map:
		ret = invokeMap(vType, reflect.ValueOf(v.Interface()))
	case reflect.Slice:
		ret = invokeSlice(vType, reflect.ValueOf(v.Interface()))
	default:
		ret = reflect.ValueOf(v.Interface())
	}
	return ret
}

//解析map
func invokeMap(mapType reflect.Type, mapValue reflect.Value) reflect.Value {
	result := reflect.MakeMapWithSize(mapType, len(mapValue.MapKeys()))
	keyType := mapType.Key()
	valueType := mapType.Elem()

	//fmt.Println("   keyType:", keyType, " valueType:", valueType)
	for _, keyValue := range mapValue.MapKeys() { //遍历map
		value := mapValue.MapIndex(keyValue)
		if keyType.Kind() == reflect.Interface {
			keyValue = invokeValue(keyType, keyValue)
		}
		if value.Type().Kind() == reflect.Interface {
			value = invokeValue(valueType, value)
		}
		result.SetMapIndex(keyValue, value)
	}

	return result
}

//解析slice
func invokeSlice(sliceType reflect.Type, sliceValue reflect.Value) reflect.Value {
	sv := sliceValue.Interface().([]interface{});
//	fmt.Println("sT",sliceType.Elem())
	result := reflect.MakeSlice(sliceType, 0, 0)
	//len(sv))
	for _, v := range sv {
		rv := invokeValue(sliceType.Elem(), reflect.ValueOf(v))
		result = reflect.Append(result, rv)
	}
	//fmt.Println("sliceType:", sliceType)
	//fmt.Println("sliceVale:", reflect.TypeOf(sliceValue.Interface()))
	//fmt.Println("sliceVale:", sliceValue.Type())

	//result = reflect.Append(result, reflect.ValueOf(parsedValue))
	return  result
}

//运行Worker
func RunWorker(queueName string, redis rds.Conn) bool {
	res, err := rds.String(redis.Do("RPOP", queueName))
	if err != nil {
		fmt.Println("reids error: ", err)
		return false
	}

	if strings.TrimSpace(res) == "" {
		fmt.Println("not found data")
		return false
	}

	p := Payload{}
	err = json.Unmarshal([]byte(res), &p)
	if err != nil {
		fmt.Println(err)
		return false
	}

	//获取Worker类型
	r, ret := newWorkerInstance(p.Class)
	if ret {
		//通过反射call Run方法
		var a interface{}
		err := json.Unmarshal([]byte(p.Args), &a)

		if err == nil {
			//参数是map类型
			Args := make(map[string]interface{}, 0)
			//kType := reflect.TypeOf(a).Key()
			if reflect.TypeOf(a).Kind() == reflect.Map {
				arf_v := reflect.ValueOf(a)
				for _, k := range arf_v.MapKeys() { //遍历map
					Args[k.String()] = arf_v.MapIndex(k).Interface()
				}
			}

			v := r.Elem() //a需要是引用
			k := v.Type()

			for i := 0; i < v.NumField(); i++ {
				key := k.Field(i)
				val := v.Field(i)
				//fmt.Println(key.Name, val.Type(), val.Interface(), val.CanSet())
				if val.CanSet() {
					switch(val.Type().Kind()) {
					case reflect.Bool:
						vv := Args[key.Name].(bool)
						val.SetBool(vv)
					case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
						vv := int64(Args[key.Name].(float64))
						val.SetInt(vv)
					case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
						vv := int64(Args[key.Name].(float64))
						val.SetInt(vv)
					case reflect.Float32, reflect.Float64:
						vv := Args[key.Name].(float64)
						val.SetFloat(vv)
					case reflect.String:
						val.SetString(Args[key.Name].(string))
					case reflect.Map:
						result := invokeMap(val.Type(), reflect.ValueOf(Args[key.Name]))
						val.Set(result)
					case reflect.Slice:
						result := invokeSlice(val.Type(), reflect.ValueOf(Args[key.Name]))
						val.Set(result)
//					default :
//						val.Set(reflect.ValueOf(Args[key.Name]))
					}

				}
			}
			r.MethodByName("Run").Call([]reflect.Value{})
			return true
		}
	}
	return false
}