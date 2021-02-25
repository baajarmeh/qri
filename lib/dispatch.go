package lib

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/qri-io/qfs"
)


func (inst *Instance) Dispatch(ctx context.Context, method string, param interface{}) (interface{}, error) {

	var err error
	var res interface{}

	// TODO(dustmop): In reality, this should be done once, at startup, for long-lived processes
	reg := inst.registerImplementations()

	//
	if inst.http != nil {
		// TODO(dustmop): This is broken, should instead forward the `method,param` tuple
		// across this http client
		err = inst.http.Call(ctx, AEApply, param, res)
		if err != nil {
			return nil, err
		}
		return res, nil
	}

	if c, ok := reg[method]; ok {
		scope := Scope{
			ctx:  ctx,
			inst: inst,
		}

		args := make([]reflect.Value, 3)
		// Impl
		args[0] = reflect.ValueOf(c.Impl)
		// Scope
		args[1] = reflect.ValueOf(scope)
		// Param
		// TODO(dustmop): Validate that param matches InType
		// TODO(dustmop): Clone param to args[2]?
		input := reflect.ValueOf(param)
		inStruct := input.Elem()
		if input.Kind() == reflect.Struct {
			typeStruct := input.Type().Elem()
			numFields := typeStruct.NumField()
			for k := 0; k < numFields; k++ {
				eachValue := ""
				field := typeStruct.Field(k)
				fieldTag := field.Tag
				qriTag := fieldTag.Get("qri")
				valueField := inStruct.Field(k)
				if qriTag != "" {
					// TODO(dustmop): Validate that the field is of type string
					if qriTag == "filepath" {
						inf := valueField.Interface()
						textPath := inf.(string)
						eachValue = fmt.Sprintf(", value: %q", textPath)
						_ = qfs.AbsPath(&textPath)
						valueField.SetString(textPath)
					} else {
						fmt.Printf("unknown tag: %s\n", qriTag)
					}
				}
				fmt.Printf("%d: %q qriTag: %s %s\n", k, field.Name, qriTag, eachValue)
			}
		}
		args[2] = input

		fmt.Printf("c.Func starting\n")

		outVals := c.Func.Call(args)

		fmt.Printf("c.Func done\n")

		if len(outVals) != 2 {
			fmt.Printf("wrong number of return args: %d\n", len(outVals))
			return nil, fmt.Errorf("bad")
		} else {
			// Correct number of values
			var out interface{}
			out = outVals[0].Interface()
			errVal := outVals[1].Interface()
			if errVal == nil {
				return out, nil
			}

			if err, ok := errVal.(error); ok {
				return out, err
			} else {
				fmt.Printf("could not convert to err: %v\n", errVal)
				return nil, fmt.Errorf("bad")
			}
		}
	}
	return nil, fmt.Errorf("method %q not found", method)
}

type callable struct {
	Impl interface{}
	Func reflect.Value
	InType reflect.Type
	OutType reflect.Type
}

func (inst *Instance) registerImplementations() map[string]callable {
	reg := make(map[string]callable)
	inst.registerOne("fsi", &FSIImpl{}, reg)
	return reg
}

func (inst *Instance) registerOne(ourName string, impl interface{}, reg map[string]callable) {
	//reg[name] = impl
	v := reflect.TypeOf(impl)
	num := v.NumMethod()
	fmt.Printf("%d methods\n", num)
	for k := 0; k < num; k++ {
		m := v.Method(k)
		fmt.Printf("%d: %s, %s\n", k, m.Name, m.Type)
		lowerName := strings.ToLower(m.Name)
		funcName := ourName + "." + lowerName

		f := m.Type

		if f.NumIn() != 3 {
			fmt.Printf("Error: bad number of in args: %d\n", f.NumIn())
			continue
		}
		if f.NumOut() != 2 {
			fmt.Printf("Error: bad number of out args: %d\n", f.NumOut())
			continue
		}
		// TODO(dustmop): Validate each arguments type. Especially, this must be a pointer
		inType := f.In(2).Elem()
		outType := f.Out(0)

		reg[funcName] = callable{
			Impl: impl,
			Func: m.Func,
			InType: inType,
			OutType: outType,
		}
		fmt.Printf("registered %q, in %v, out %v\n", funcName, inType, outType)
	}
}
