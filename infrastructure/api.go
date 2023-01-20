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
