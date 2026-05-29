package config

import "fmt"

const NumServers = 5

func Address(id int) string {
	return fmt.Sprintf("127.0.0.1:%d", 9000+id)
}

func DockerAddress(id int) string {
	return fmt.Sprintf("server%d:%d", id, 9000+id)
}

func AllAddresses(docker bool) map[int]string {
	m := map[int]string{}
	for i := 1; i <= NumServers; i++ {
		if docker {
			m[i] = DockerAddress(i)
		} else {
			m[i] = Address(i)
		}
	}
	return m
}
