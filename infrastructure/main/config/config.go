package config

import (
	"io/ioutil"

	"github.com/BurntSushi/toml"
)

func ReadTOMLConfig(fname string, conf interface{}) error {
	data, err := ioutil.ReadFile(fname)
	if err != nil {
		return err
	}

	_, err = toml.Decode(string(data), conf)
	if err != nil {
		return err
	}
	return nil
}
