package infrastructure

const (
	CrowAllocateAPIResource = "/endpoint/allocate"

	CrowServiceAPIPublishResource = "/publish"
	CrowServiceAPIPurgeResource   = "/purge"
)

const (
	CyprusServiceAPIProcessResource = "/process"
	CyprusServiceAPIStatusResource  = "/status"
	CyprusServiceAPIDeleteResource  = "/delete"

	CyprusStorageAPIKeyResource              = "/key"
	CyprusStorageAPIDataResource             = "/crypdata"
	CyprusStorageAPIPartialMetadataResource  = "/metadata/partial"
	CyprusStorageAPICompleteMetadataResource = "/metadata/complete"
)

const (
	DamoclesSignalAPIMatchResource = "/client/match"
	DamoclesSignalAPIPlaceResource = "/endpoint/place"

	DamoclesServiceAPIAddResource = "/category/add"
	DamoclesServiceAPIDelResource = "/category/del"
)

const (
	AmadaRouteAPIClientResource   = "/route/client"
	AmadaRouteAPIEndpointResource = "/route/endpoint"

	AmadaServiceAPISetRegionResource = "/region/set"
	AmadaServiceAPIDelRegionResource = "/region/del"
	AmadaServiceAPIUpdateGeoResource = "/geomap/update"
)

const (
	DeusServiceAPIPullDeciderResource = "/decider/update"

	DeusServiceAPIStaleReportResource = "/content/stale"
	DeusServiceAPIPushResource        = "/content/push"
	DeusServiceAPIPurgeResource       = "/content/purge"
)

const (
	DominiqueReportAPIClientResource   = "/client/report"
	DominiqueReportAPIEndpointResource = "/endpoint/report"

	DominiqueDataAPIFetchResource = "/fetch"
)

const (
	ReikoServiceAPIValidateResource = "/content/validate"
	ReikoServiceAPIAddRuleResource  = "/rule/add"
	ReikoServiceAPIDelRuleResource  = "/rule/del"
)

const (
	// Node Reporting Resources
	LeviReportAPIStaleResource           = "/report/stale"
	LeviReportAPIEndpointSessionResource = "/report/endpoint"
	LeviReportAPIClientSessionResource   = "/report/client"

	// CDN Modification Resources
	LeviContentAPIPullAddResource    = "/pull/add"
	LeviContentAPIPullRemoveResource = "/pull/remove"
	LeviContentAPIPushAddResource    = "/push/add"
	LeviContentAPIPushRemoveResource = "/push/remove"

	// Data Retrieval Resources
	LeviDataAPIFetchResource = "/data/fetch"
)

const (
	// Region-to-Server mapping resources
	StateAPIGetRegionResource    = "/region/get"
	StateAPISetRegionResource    = "/region/set"
	StateAPIDeleteRegionResource = "/region/delete"

	// Content metadata resources
	StateAPIGetFunctionalIDResource     = "/content/fid/get"
	StateAPIGetContentIDResource        = "/content/cid/get"
	StateAPIGetContentResourcesResource = "/content/resources/get"
	StateAPIGetContentSizeResource      = "/content/size/get"
	StateAPICreateContentEntryResource  = "/content/create"
	StateAPIDeleteContentEntryResource  = "/content/delete"

	// Edge network server entry resources
	StateAPICreateServerEntryResource       = "/server/create"
	StateAPIDeleteServerEntryResource       = "/server/delete"
	StateAPIGetServerPublicAddressResource  = "/server/public"
	StateAPIGetServerPrivateAddressResource = "/server/private"

	// Edge network content state resources
	StateAPIGetServerListResource              = "/server/list"
	StateAPIIsServerServingResource            = "/server/cid/exists"
	StateAPIGetContentServerListResource       = "/content/cid/servers"
	StateAPIGetServerContentListResource       = "/server/cid/list"
	StateAPIIsContentActiveResource            = "/content/cid/active"
	StateAPIWasContentPulledResource           = "/server/cid/pulled"
	StateAPICreateContentLocationEntryResource = "/server/cid/create"
	StateAPIDeleteContentLocationEntryResource = "/server/cid/delete"

	// Dynamically pulled content rule resource
	StateAPIGetContentPullRulesResource   = "/rules/all"
	StateAPIDoesRuleExistResource         = "/rules/exists"
	StateAPICreateContentPullRuleResource = "/rules/create"
	StateAPIDeleteContentPullRuleResource = "/rules/delete"
)
