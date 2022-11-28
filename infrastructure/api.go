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
)

const (
	DamoclesSignalAPIMatchResource = "/client/match"
	DamoclesSignalAPIPlaceResource = "/endpoint/place"

	DamoclesServiceAPIAddResource = "/category/add"
	DamoclesServiceAPIDelResource = "/category/del"
)

const (
	DeusRouteAPIClientResource   = "/route/client"
	DeusRouteAPIEndpointResource = "/route/endpoint"

	DeusServiceAPIPushResource      = "/content/push"
	DeusServiceAPIPurgeResource     = "/content/purge"
	DeusServiceAPISetRegionResource = "/region/set"
	DeusServiceAPIDelRegionResource = "/region/del"
	DeusServiceAPIUpdateGeoResource = "/geomap/update"
)

const (
	DominiqueReportAPIClientResource   = "/client/report"
	DominiqueReportAPIEndpointResource = "/endpoint/report"
)

const (
	ReikoServiceAPIValidateResource = "/content/validate"
	ReikoServiceAPIAddRuleResource  = "/rule/add"
	ReikoServiceAPIDelRuleResource  = "/rule/del"
)
