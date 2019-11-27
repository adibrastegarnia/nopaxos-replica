// Copyright 2019-present Open Networking Foundation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import "time"

const (
	defaultLeaderTimeout      = 5 * time.Second
	defaultPingInterval       = 500 * time.Millisecond
	defaultCheckpointInterval = 5 * time.Minute
	defaultSyncInterval       = 5 * time.Minute
	defaultMaxLogLength       = 10000
)

// GetLeaderTimeoutOrDefault returns the configured election timeout if set, otherwise the default election timeout
func (c *ProtocolConfig) GetLeaderTimeoutOrDefault() time.Duration {
	timeout := c.GetLeaderTimeout()
	if timeout != nil {
		return *timeout
	}
	return defaultLeaderTimeout
}

// GetPingIntervalOrDefault returns the configured heartbeat interval if set, otherwise the default heartbeat interval
func (c *ProtocolConfig) GetPingIntervalOrDefault() time.Duration {
	interval := c.GetPingInterval()
	if interval != nil {
		return *interval
	}
	return defaultPingInterval
}

// GetCheckpointIntervalOrDefault returns the configured checkpoint interval if set, otherwise the default interval
func (c *ProtocolConfig) GetCheckpointIntervalOrDefault() time.Duration {
	interval := c.GetCheckpointInterval()
	if interval != nil {
		return *interval
	}
	return defaultCheckpointInterval
}

// GetSyncIntervalOrDefault returns the configured sync interval if set, otherwise the default interval
func (c *ProtocolConfig) GetSyncIntervalOrDefault() time.Duration {
	interval := c.GetSyncInterval()
	if interval != nil {
		return *interval
	}
	return defaultSyncInterval
}

// GetMaxLogLengthOrDefault returns the configured maximum log length, otherwise the default
func (c *ProtocolConfig) GetMaxLogLengthOrDefault() int {
	length := c.GetMaxLogLength()
	if length > 0 {
		return int(length)
	}
	return defaultMaxLogLength
}
