package controlplaneevents

func (ev *Event) GetEventName() string {
	switch ev.GetEvent().(type) {
	case *Event_ExecutorSettingsUpsert:
		return "ExecutorSettingsUpsert"
	case *Event_ExecutorSettingsDelete:
		return "ExecutorSettingsDelete"
	}
	return "Unknown"
}
