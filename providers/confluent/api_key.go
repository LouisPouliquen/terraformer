package confluent

import (
	"context"
	"fmt"
	"github.com/GoogleCloudPlatform/terraformer/terraformutils"
	apikeys "github.com/confluentinc/ccloud-sdk-go-v2/apikeys/v2"
)

type ApiKeyGenerator struct {
	ConfluentService
}

func (g ApiKeyGenerator) createResources(apiKeys apikeys.IamV2ApiKeyList) []terraformutils.Resource {
	var resources []terraformutils.Resource
	for _, apiKey := range apiKeys.Data {
		resources = append(resources, terraformutils.NewSimpleResource(
			apiKey.GetId(),
			apiKey.Spec.Owner.GetId(),
			"confluent_api_key",
			"confluent",
			[]string{}))
	}
	return resources
}

func (g *ApiKeyGenerator) InitResources() error {
	ctx := context.Background()
	apiKeys, _, err := g.createClient().apiKeysClient.APIKeysIamV2Api.ListIamV2ApiKeys(g.ApiKeysApiContext(ctx)).Execute()
	if err != nil {
		return fmt.Errorf("unable to list api keys : %v", err)
	}

	g.createResources(apiKeys)
	if err != nil {
		return err
	}

	return nil
}
