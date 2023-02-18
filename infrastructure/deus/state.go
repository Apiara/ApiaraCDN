package deus

import "github.com/Apiara/ApiaraCDN/infrastructure/state"

type ManagerMicroserviceState interface {
	state.ServerStateReader
	state.ContentLocationState
	state.ContentMetadataState
}
