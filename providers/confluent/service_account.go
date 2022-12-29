package confluent

import (
	"context"
	"fmt"
	"github.com/GoogleCloudPlatform/terraformer/terraformutils"
	v2 "github.com/confluentinc/ccloud-sdk-go-v2/iam/v2"
	"net/http"
)

const (
	// The maximum allowable page size - 1 (to avoid off-by-one errors) when listing service accounts using IAM V2 API
	// https://docs.confluent.io/cloud/current/api.html#operation/listIamV2ServiceAccounts
	listServiceAccountsPageSize = 99
	pageTokenQueryParameter     = "page_token"
)

type ServiceAccountGenerator struct {
	ConfluentService
}

func (g ServiceAccountGenerator) createResources(sas []v2.IamV2ServiceAccount) []terraformutils.Resource {
	var resources []terraformutils.Resource
	for _, sa := range sas {
		resources = append(resources, terraformutils.NewSimpleResource(
			sa.GetId(),
			sa.GetDisplayName(),
			"confluent_service_account",
			"confluent",
			[]string{}))
	}
	return resources
}

func (g *ServiceAccountGenerator) InitResources() error {
	ctx := context.Background()
	serviceAccounts := make([]v2.IamV2ServiceAccount, 0)

	allServiceAccountsAreCollected := false
	pageToken := ""
	for !allServiceAccountsAreCollected {
		serviceAccountPageList, _, err := g.executeListServiceAccounts(ctx, pageToken)
		if err != nil {
			return fmt.Errorf("error reading Service Accounts: %s", err)
		}
		serviceAccounts = append(serviceAccounts, serviceAccountPageList.GetData()...)

		// nextPageUrlStringNullable is nil for the last page
		nextPageUrlStringNullable := serviceAccountPageList.GetMetadata().Next

		if nextPageUrlStringNullable.IsSet() {
			nextPageUrlString := *nextPageUrlStringNullable.Get()
			if nextPageUrlString == "" {
				allServiceAccountsAreCollected = true
			} else {
				pageToken, err = extractPageToken(nextPageUrlString)
				if err != nil {
					return fmt.Errorf("error reading Service Accounts: %s", err)
				}
			}
		} else {
			allServiceAccountsAreCollected = true
		}
	}
	g.Resources = g.createResources(serviceAccounts)
	return nil
}

func (g *ServiceAccountGenerator) executeListServiceAccounts(ctx context.Context, pageToken string) (v2.IamV2ServiceAccountList, *http.Response, error) {
	if pageToken != "" {
		return g.createClient().iamClient.ServiceAccountsIamV2Api.ListIamV2ServiceAccounts(g.iamApiContext(ctx)).PageSize(listServiceAccountsPageSize).PageToken(pageToken).Execute()
	} else {
		return g.createClient().iamClient.ServiceAccountsIamV2Api.ListIamV2ServiceAccounts(g.iamApiContext(ctx)).PageSize(listServiceAccountsPageSize).Execute()
	}
}
