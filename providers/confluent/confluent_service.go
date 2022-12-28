package confluent

import (
	"github.com/GoogleCloudPlatform/terraformer/terraformutils"
	apikeys "github.com/confluentinc/ccloud-sdk-go-v2/apikeys/v2"
	cmk "github.com/confluentinc/ccloud-sdk-go-v2/cmk/v2"
	connect "github.com/confluentinc/ccloud-sdk-go-v2/connect/v1"
	iamv1 "github.com/confluentinc/ccloud-sdk-go-v2/iam/v1"
	iam "github.com/confluentinc/ccloud-sdk-go-v2/iam/v2"
	oidc "github.com/confluentinc/ccloud-sdk-go-v2/identity-provider/v2"
	quotas "github.com/confluentinc/ccloud-sdk-go-v2/kafka-quotas/v1"
	kafkarestv3 "github.com/confluentinc/ccloud-sdk-go-v2/kafkarest/v3"
	ksql "github.com/confluentinc/ccloud-sdk-go-v2/ksql/v2"
	mds "github.com/confluentinc/ccloud-sdk-go-v2/mds/v2"
	net "github.com/confluentinc/ccloud-sdk-go-v2/networking/v1"
	org "github.com/confluentinc/ccloud-sdk-go-v2/org/v2"
	schemaregistry "github.com/confluentinc/ccloud-sdk-go-v2/schema-registry/v1"
	srcm "github.com/confluentinc/ccloud-sdk-go-v2/srcm/v2"
)

type ConfluentService struct {
	terraformutils.Service
}

type Client struct {
	apiKeysClient        *apikeys.APIClient
	iamClient            *iam.APIClient
	iamV1Client          *iamv1.APIClient
	cmkClient            *cmk.APIClient
	connectClient        *connect.APIClient
	netClient            *net.APIClient
	orgClient            *org.APIClient
	ksqlClient           *ksql.APIClient
	kafkaClient          *kafkarestv3.APIClient
	schemaRegistryClient *schemaregistry.APIClient
	mdsClient            *mds.APIClient
	oidcClient           *oidc.APIClient
	quotasClient         *quotas.APIClient
	srcmClient           *srcm.APIClient
	userAgent            string
}

func (s ConfluentService) createClient() *Client {

	endpoint := s.Args["endpoint"].(string)
	maxRetries := s.Args["max_retries"].(int)

	apiKeysCfg := apikeys.NewConfiguration()
	cmkCfg := cmk.NewConfiguration()
	connectCfg := connect.NewConfiguration()
	iamCfg := iam.NewConfiguration()
	iamV1Cfg := iamv1.NewConfiguration()
	mdsCfg := mds.NewConfiguration()
	netCfg := net.NewConfiguration()
	oidcCfg := oidc.NewConfiguration()
	orgCfg := org.NewConfiguration()
	srcmCfg := srcm.NewConfiguration()
	ksqlCfg := ksql.NewConfiguration()
	quotasCfg := quotas.NewConfiguration()
	kafkaCfg := kafkarestv3.NewConfiguration()
	schemaCfg := schemaregistry.NewConfiguration()

	apiKeysCfg.Servers[0].URL = endpoint
	cmkCfg.Servers[0].URL = endpoint
	connectCfg.Servers[0].URL = endpoint
	iamCfg.Servers[0].URL = endpoint
	iamV1Cfg.Servers[0].URL = endpoint
	mdsCfg.Servers[0].URL = endpoint
	netCfg.Servers[0].URL = endpoint
	oidcCfg.Servers[0].URL = endpoint
	orgCfg.Servers[0].URL = endpoint
	srcmCfg.Servers[0].URL = endpoint
	ksqlCfg.Servers[0].URL = endpoint
	quotasCfg.Servers[0].URL = endpoint
	kafkaCfg.Servers[0].URL = endpoint
	schemaCfg.Servers[0].URL = endpoint

	if maxRetries != 0 {
		apiKeysCfg.HTTPClient = NewRetryableClientFactory(WithMaxRetries(maxRetries)).CreateRetryableClient()
		cmkCfg.HTTPClient = NewRetryableClientFactory(WithMaxRetries(maxRetries)).CreateRetryableClient()
		connectCfg.HTTPClient = NewRetryableClientFactory(WithMaxRetries(maxRetries)).CreateRetryableClient()
		iamCfg.HTTPClient = NewRetryableClientFactory(WithMaxRetries(maxRetries)).CreateRetryableClient()
		iamV1Cfg.HTTPClient = NewRetryableClientFactory(WithMaxRetries(maxRetries)).CreateRetryableClient()
		mdsCfg.HTTPClient = NewRetryableClientFactory(WithMaxRetries(maxRetries)).CreateRetryableClient()
		netCfg.HTTPClient = NewRetryableClientFactory(WithMaxRetries(maxRetries)).CreateRetryableClient()
		oidcCfg.HTTPClient = NewRetryableClientFactory(WithMaxRetries(maxRetries)).CreateRetryableClient()
		orgCfg.HTTPClient = NewRetryableClientFactory(WithMaxRetries(maxRetries)).CreateRetryableClient()
		srcmCfg.HTTPClient = NewRetryableClientFactory(WithMaxRetries(maxRetries)).CreateRetryableClient()
		ksqlCfg.HTTPClient = NewRetryableClientFactory(WithMaxRetries(maxRetries)).CreateRetryableClient()
		quotasCfg.HTTPClient = NewRetryableClientFactory(WithMaxRetries(maxRetries)).CreateRetryableClient()
		kafkaCfg.HTTPClient = NewRetryableClientFactory(WithMaxRetries(maxRetries)).CreateRetryableClient()
		schemaCfg.HTTPClient = NewRetryableClientFactory(WithMaxRetries(maxRetries)).CreateRetryableClient()
	} else {
		connectCfg.HTTPClient = NewRetryableClientFactory().CreateRetryableClient()
		apiKeysCfg.HTTPClient = NewRetryableClientFactory().CreateRetryableClient()
		cmkCfg.HTTPClient = NewRetryableClientFactory().CreateRetryableClient()
		iamCfg.HTTPClient = NewRetryableClientFactory().CreateRetryableClient()
		iamV1Cfg.HTTPClient = NewRetryableClientFactory().CreateRetryableClient()
		mdsCfg.HTTPClient = NewRetryableClientFactory().CreateRetryableClient()
		netCfg.HTTPClient = NewRetryableClientFactory().CreateRetryableClient()
		oidcCfg.HTTPClient = NewRetryableClientFactory().CreateRetryableClient()
		orgCfg.HTTPClient = NewRetryableClientFactory().CreateRetryableClient()
		srcmCfg.HTTPClient = NewRetryableClientFactory().CreateRetryableClient()
		ksqlCfg.HTTPClient = NewRetryableClientFactory().CreateRetryableClient()
		quotasCfg.HTTPClient = NewRetryableClientFactory().CreateRetryableClient()
		kafkaCfg.HTTPClient = NewRetryableClientFactory().CreateRetryableClient()
		schemaCfg.HTTPClient = NewRetryableClientFactory().CreateRetryableClient()
	}
	return &Client{
		apiKeysClient:        apikeys.NewAPIClient(apiKeysCfg),
		iamClient:            iam.NewAPIClient(iamCfg),
		iamV1Client:          iamv1.NewAPIClient(iamV1Cfg),
		cmkClient:            cmk.NewAPIClient(cmkCfg),
		connectClient:        connect.NewAPIClient(connectCfg),
		netClient:            net.NewAPIClient(netCfg),
		orgClient:            org.NewAPIClient(orgCfg),
		ksqlClient:           ksql.NewAPIClient(ksqlCfg),
		kafkaClient:          kafkarestv3.NewAPIClient(kafkaCfg),
		schemaRegistryClient: schemaregistry.NewAPIClient(schemaCfg),
		mdsClient:            mds.NewAPIClient(mdsCfg),
		oidcClient:           oidc.NewAPIClient(oidcCfg),
		quotasClient:         quotas.NewAPIClient(quotasCfg),
		srcmClient:           srcm.NewAPIClient(srcmCfg),
	}
}
