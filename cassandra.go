package cassandra

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/machinebox/graphql"
	"github.com/nicelogic/config"
)

type Client struct {
	userName      string
	pwd           string
	authUrl       string
	graphqlUrl    string
	token         string
	graphqlClient *graphql.Client
}

type cassandraConfig struct {
	Cassandra_auth_url    string
	Cassandra_graphql_url string
}

func (cassandra *Client) Init(keyspace string) (err error) {
	byteUserName, err := os.ReadFile("/etc/app-0/secret-cassandra/username")
	if err != nil {
		return
	}
	cassandra.userName = strings.TrimSpace(string(byteUserName))
	fmt.Printf("userName: %s\n", cassandra.userName)

	bytePwd, err := os.ReadFile("/etc/app-0/secret-cassandra/password")
	if err != nil {
		return
	}
	cassandra.pwd = strings.TrimSpace(string(bytePwd))

	aConfig := cassandraConfig{}
	err = config.Init("/etc/app-0/config/config.yml", &aConfig)
	if err != nil {
		return
	}
	cassandra.authUrl = aConfig.Cassandra_auth_url
	cassandra.graphqlUrl = aConfig.Cassandra_graphql_url + keyspace
	fmt.Printf("authUrl: %s\n", cassandra.authUrl)
	fmt.Printf("authGraphqlUrl: %s\n", cassandra.graphqlUrl)

	cassandra.graphqlClient = graphql.NewClient(cassandra.graphqlUrl)
	return
}

func (cassandra *Client) fetchToken() (err error) {
	auth := map[string]string{"username": cassandra.userName, "password": cassandra.pwd}
	body, err := json.Marshal(auth)
	if err != nil {
		return
	}
	response, err := http.Post(cassandra.authUrl, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return
	}

	var res map[string]interface{}
	err = json.NewDecoder(response.Body).Decode(&res)
	if err != nil {
		return
	}
	token, ok := res["authToken"].(string)
	if !ok || token == "" {
		err = fmt.Errorf("%s response not contain token", cassandra.authUrl)
		return
	}
	cassandra.token = token
	fmt.Printf("token: %v\n", cassandra.token)
	return
}

func (cassandra *Client) Run(gql string, variables map[string]interface{}) (response map[string]interface{}, err error) {
	if cassandra.token == "" {
		fmt.Printf("token is empty, will fetch\n")
		err = cassandra.fetchToken()
		if err != nil{
			return 
		}
	}

	req := graphql.NewRequest(gql)
	for key, value := range variables {
		req.Var(key, value)
	}
	req.Header.Set("x-cassandra-token", cassandra.token)
	ctx := context.Background()
	// token will exipired after 30 minues if no any operation
	// if token expired, need fetch token, then redo
	if err = cassandra.graphqlClient.Run(ctx, req, &response); err != nil {
		fmt.Printf("%v\n", err)
		fmt.Println("token may expired, fetch token, try again")
		err = cassandra.fetchToken()
		if err != nil {
			return 
		}
		req.Header.Set("x-cassandra-token", cassandra.token)
		if err = cassandra.graphqlClient.Run(ctx, req, &response); err != nil {
			fmt.Printf("%v", err)
			return 
		}
	}
	return
}

func (cassandra *Client) Mutation(gql string, variables map[string]interface{}) (response map[string]interface{}, err error) {
	response, err = cassandra.Run(gql, variables)
	return
}

func (cassandra *Client) Query(gql string, variables map[string]interface{}) (response map[string]interface{}, err error) {
	response, err = cassandra.Run(gql, variables)
	return
}
