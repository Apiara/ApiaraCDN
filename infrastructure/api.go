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
)

const (
	ReikoServiceAPIValidateResource = "/content/validate"
	ReikoServiceAPIAddRuleResource  = "/rule/add"
	ReikoServiceAPIDelRuleResource  = "/rule/del"
)
