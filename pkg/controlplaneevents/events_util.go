package controlplaneevents

func (ev *Event) GetEventName() string {
	switch ev.GetEvent().(type) {
	case *Event_ExecutorSettingsUpsert:
		return "ExecutorSettingsUpsert"
	case *Event_ExecutorSettingsDelete:
		return "ExecutorSettingsDelete"
	case *Event_ExecutorDelete:
		return "ExecutorDelete"
	}
	return "Unknown"
}
