package mr

func stringInSlice(a string, list []MapTask) bool {
	for _, b := range list {
		if b.Name == a {
			return true
		}
	}
	return false
}
