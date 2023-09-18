package main

func main() {
	type key struct {
		v1 string
		v2 string
	}

	k1 := key{
		v1: "a",
		v2: "b",
	}

	k2 := key{
		v1: "a",
		v2: "b",
	}

	k3 := key{
		v1: "c",
		v2: "d",
	}

	k4 := key{
		v1: "e",
		v2: "f",
	}

	metrics := map[key]int{
		k1: 10,
		k3: 20,
	}

	println(metrics[k1])
	println(metrics[k2])
	println(metrics[k3])
	println(metrics[k4])
}
