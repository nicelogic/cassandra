package cassandra

import (
	"fmt"
	"testing"
)

func TestCassandraInit(t *testing.T){
	aCassandra := Client{}
	err := aCassandra.Init("app_0_user")
	if err != nil{
		t.Errorf("error: %s", err)
		return
	}
	rightAuthUrl := "https://auth.cassandra.env0.luojm.com:9443/v1/auth"
	if aCassandra.authUrl != rightAuthUrl{
		t.Errorf("authUrl is wrong, want: %s, but: %s", rightAuthUrl, aCassandra.authUrl)
	}
}

func TestFetchToken(t *testing.T){
	aCassandra := Client{}
	err := aCassandra.Init("app_0_user")
	if err != nil{
		t.Errorf("error: %s", err)
		return
	}

	err = aCassandra.fetchToken()
	if err != nil{
		t.Errorf("error: %s", err)
		return
	}
	if aCassandra.token == ""{
		t.Error("after get token, token still empty")
	}
}

func TestMutation(t *testing.T){
	cassandraClient := Client{}
	err := cassandraClient.Init("app_0_user")
	if err != nil{
		t.Errorf("error: %s", err)
		return
	}

	const gql = `mutation updateUser($id: String!, $update_time: Timestamp!, $name: String, $signature: String) {
		updateUserName: updateuser(value: {
										  id: $id
						  name:$name , 
										  signature: $signature
						  update_time: $update_time
						},
									  ifExists: false,
						
						)
		{
				applied,
				accepted,
				value {
				  id,
				  name,
				  signature,
				  update_time
	  
				}
			  }
	  }`

	variables := map[string]interface{}{
		"id": "1xCC4ad7tquoyGpRUMMNt",
  		"update_time": "2022-10-22T04:03:18.879Z",
		"name": "hi",
		"signature": "hi",
	}
	response, err := cassandraClient.Mutation(gql, variables)
	if err != nil{
		t.Errorf("has err: %v", err)
		return
	}
	fmt.Printf("%v", response)
}
