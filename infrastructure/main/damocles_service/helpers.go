package main

import "github.com/Apiara/ApiaraCDN/infrastructure/damocles"

// implements damocles.CategoryUpdater for need based damocles implementations
type categoryUpdater struct {
	conns   damocles.ConnectionManager
	tracker damocles.NeedTracker
}

func (c *categoryUpdater) CreateCategory(id string) error {
	if err := c.conns.CreateCategory(id); err != nil {
		return err
	}
	if err := c.tracker.CreateCategory(id); err != nil {
		return err
	}
	return nil
}

func (c *categoryUpdater) DelCategory(id string) error {
	if err := c.conns.DelCategory(id); err != nil {
		return err
	}
	if err := c.tracker.DelCategory(id); err != nil {
		return err
	}
	return nil
}
