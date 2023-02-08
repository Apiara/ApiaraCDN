package state

import (
	"encoding/gob"
	"log"
	"net/http"
	"strconv"

	infra "github.com/Apiara/ApiaraCDN/infrastructure"
)

const (
	RegionHeader           = "region"
	ServerHeader           = "server"
	ContentIDHeader        = "content_id"
	FunctionalIDHeader     = "functional_id"
	ContentSizeHeader      = "size"
	ContentWasPulledHeader = "pulled"
	RuleHeader             = "rule"
)

func sendViaGob(data interface{}, resp http.ResponseWriter) {
	enc := gob.NewEncoder(resp)
	if err := enc.Encode(data); err != nil {
		resp.WriteHeader(http.StatusInternalServerError)
		log.Println(err)
	}
}

type apiResourceAccumulator func(*http.ServeMux, MicroserviceState)

func setDataServiceRegionResources(mux *http.ServeMux, manager MicroserviceState) {
	mux.HandleFunc(infra.StateAPIGetRegionResource, func(resp http.ResponseWriter, req *http.Request) {
		region := req.URL.Query().Get(RegionHeader)
		server, err := manager.GetRegionAddress(region)
		if err != nil {
			resp.WriteHeader(http.StatusInternalServerError)
			log.Println(err)
			return
		}
		sendViaGob(server, resp)
	})

	mux.HandleFunc(infra.StateAPISetRegionResource, func(resp http.ResponseWriter, req *http.Request) {
		query := req.URL.Query()
		region := query.Get(RegionHeader)
		server := query.Get(ServerHeader)
		if err := manager.SetRegionAddress(region, server); err != nil {
			resp.WriteHeader(http.StatusInternalServerError)
			log.Println(err)
		}
	})

	mux.HandleFunc(infra.StateAPIDeleteRegionResource, func(resp http.ResponseWriter, req *http.Request) {
		region := req.URL.Query().Get(RegionHeader)
		if err := manager.RemoveRegionAddress(region); err != nil {
			resp.WriteHeader(http.StatusInternalServerError)
			log.Println(err)
		}
	})
}

type metadataCreate struct {
	ContentID    string
	FunctionalID string
	Size         int64
	Resources    []string
}

func setDataServiceContentMetadataResources(mux *http.ServeMux, manager MicroserviceState) {
	mux.HandleFunc(infra.StateAPIGetFunctionalIDResource, func(resp http.ResponseWriter, req *http.Request) {
		cid := req.URL.Query().Get(ContentIDHeader)
		fid, err := manager.GetContentFunctionalID(cid)
		if err != nil {
			resp.WriteHeader(http.StatusInternalServerError)
			log.Println(err)
			return
		}
		sendViaGob(fid, resp)
	})

	mux.HandleFunc(infra.StateAPIGetContentIDResource, func(resp http.ResponseWriter, req *http.Request) {
		fid := req.URL.Query().Get(FunctionalIDHeader)
		cid, err := manager.GetContentID(fid)
		if err != nil {
			resp.WriteHeader(http.StatusInternalServerError)
			log.Println(err)
			return
		}
		sendViaGob(cid, resp)
	})

	mux.HandleFunc(infra.StateAPIGetContentResourcesResource, func(resp http.ResponseWriter, req *http.Request) {
		cid := req.URL.Query().Get(ContentIDHeader)
		resources, err := manager.GetContentResources(cid)
		if err != nil {
			resp.WriteHeader(http.StatusInternalServerError)
			log.Println(err)
			return
		}
		sendViaGob(resources, resp)
	})

	mux.HandleFunc(infra.StateAPIGetContentSizeResource, func(resp http.ResponseWriter, req *http.Request) {
		cid := req.URL.Query().Get(ContentIDHeader)
		size, err := manager.GetContentSize(cid)
		if err != nil {
			resp.WriteHeader(http.StatusInternalServerError)
			log.Println(err)
			return
		}
		sendViaGob(size, resp)
	})

	gob.Register(metadataCreate{})
	mux.HandleFunc(infra.StateAPICreateContentEntryResource, func(resp http.ResponseWriter, req *http.Request) {
		dec := gob.NewDecoder(req.Body)
		var mdata metadataCreate
		if err := dec.Decode(&mdata); err != nil {
			resp.WriteHeader(http.StatusInternalServerError)
			log.Println(err)
			return
		}

		err := manager.CreateContentEntry(mdata.ContentID, mdata.FunctionalID, mdata.Size, mdata.Resources)
		if err != nil {
			resp.WriteHeader(http.StatusInternalServerError)
			log.Println(err)
			return
		}
	})

	mux.HandleFunc(infra.StateAPIDeleteContentEntryResource, func(resp http.ResponseWriter, req *http.Request) {
		cid := req.URL.Query().Get(ContentIDHeader)
		if err := manager.DeleteContentEntry(cid); err != nil {
			resp.WriteHeader(http.StatusInternalServerError)
			log.Println(err)
			return
		}
	})
}

func setDataServiceContentLocationResources(mux *http.ServeMux, manager MicroserviceState) {
	mux.HandleFunc(infra.StateAPIIsServerServingResource, func(resp http.ResponseWriter, req *http.Request) {
		query := req.URL.Query()
		cid := query.Get(ContentIDHeader)
		server := query.Get(ServerHeader)

		result, err := manager.IsContentServedByServer(cid, server)
		if err != nil {
			resp.WriteHeader(http.StatusInternalServerError)
			log.Println(err)
			return
		}
		sendViaGob(result, resp)
	})

	mux.HandleFunc(infra.StateAPIGetContentServerListResource, func(resp http.ResponseWriter, req *http.Request) {
		cid := req.URL.Query().Get(ContentIDHeader)

		resources, err := manager.ContentServerList(cid)
		if err != nil {
			resp.WriteHeader(http.StatusInternalServerError)
			log.Println(err)
			return
		}
		sendViaGob(resources, resp)
	})

	mux.HandleFunc(infra.StateAPIGetServerContentListResource, func(resp http.ResponseWriter, req *http.Request) {
		server := req.URL.Query().Get(ServerHeader)

		result, err := manager.ServerContentList(server)
		if err != nil {
			resp.WriteHeader(http.StatusInternalServerError)
			log.Println(err)
			return
		}
		sendViaGob(result, resp)
	})

	mux.HandleFunc(infra.StateAPIIsContentActiveResource, func(resp http.ResponseWriter, req *http.Request) {
		cid := req.URL.Query().Get(ContentIDHeader)

		result, err := manager.IsContentBeingServed(cid)
		if err != nil {
			resp.WriteHeader(http.StatusInternalServerError)
			log.Println(err)
			return
		}
		sendViaGob(result, resp)
	})

	mux.HandleFunc(infra.StateAPIWasContentPulledResource, func(resp http.ResponseWriter, req *http.Request) {
		query := req.URL.Query()
		cid := query.Get(ContentIDHeader)
		server := query.Get(ServerHeader)

		result, err := manager.WasContentPulled(cid, server)
		if err != nil {
			resp.WriteHeader(http.StatusInternalServerError)
			log.Println(err)
			return
		}
		sendViaGob(result, resp)
	})

	mux.HandleFunc(infra.StateAPICreateContentLocationEntryResource, func(resp http.ResponseWriter, req *http.Request) {
		query := req.URL.Query()
		cid := query.Get(ContentIDHeader)
		server := query.Get(ServerHeader)
		pulled, err := strconv.ParseBool(query.Get(ContentWasPulledHeader))
		if err != nil {
			resp.WriteHeader(http.StatusInternalServerError)
			log.Println(err)
			return
		}

		if err := manager.CreateContentLocationEntry(cid, server, pulled); err != nil {
			resp.WriteHeader(http.StatusInternalServerError)
			log.Println(err)
		}
	})

	mux.HandleFunc(infra.StateAPIDeleteContentLocationEntryResource, func(resp http.ResponseWriter, req *http.Request) {
		query := req.URL.Query()
		cid := query.Get(ContentIDHeader)
		server := query.Get(ServerHeader)

		if err := manager.DeleteContentLocationEntry(cid, server); err != nil {
			resp.WriteHeader(http.StatusInternalServerError)
			log.Println(err)
		}
	})
}

func setDataServiceContentPullRuleResources(mux *http.ServeMux, manager MicroserviceState) {
	mux.HandleFunc(infra.StateAPIGetContentPullRulesResource, func(resp http.ResponseWriter, req *http.Request) {
		rules, err := manager.GetContentPullRules()
		if err != nil {
			resp.WriteHeader(http.StatusInternalServerError)
			log.Println(err)
			return
		}
		sendViaGob(rules, resp)
	})

	mux.HandleFunc(infra.StateAPIDoesRuleExistResource, func(resp http.ResponseWriter, req *http.Request) {
		rule := req.URL.Query().Get(RuleHeader)
		result, err := manager.ContentPullRuleExists(rule)
		if err != nil {
			resp.WriteHeader(http.StatusInternalServerError)
			log.Println(err)
			return
		}
		sendViaGob(result, resp)
	})

	mux.HandleFunc(infra.StateAPICreateContentPullRuleResource, func(resp http.ResponseWriter, req *http.Request) {
		rule := req.URL.Query().Get(RuleHeader)
		if err := manager.CreateContentPullRule(rule); err != nil {
			resp.WriteHeader(http.StatusInternalServerError)
			log.Println(err)
		}
	})

	mux.HandleFunc(infra.StateAPIDeleteContentPullRuleResource, func(resp http.ResponseWriter, req *http.Request) {
		rule := req.URL.Query().Get(RuleHeader)
		if err := manager.DeleteContentPullRule(rule); err != nil {
			resp.WriteHeader(http.StatusInternalServerError)
			log.Println(err)
		}
	})
}

func StartDataService(listenAddr string, manager MicroserviceState) {
	resources := []apiResourceAccumulator{
		setDataServiceRegionResources,
		setDataServiceContentMetadataResources,
		setDataServiceContentLocationResources,
		setDataServiceContentPullRuleResources,
	}

	serviceMux := http.NewServeMux()
	for _, accumulator := range resources {
		accumulator(serviceMux, manager)
	}
	log.Fatal(http.ListenAndServe(listenAddr, serviceMux))
}
