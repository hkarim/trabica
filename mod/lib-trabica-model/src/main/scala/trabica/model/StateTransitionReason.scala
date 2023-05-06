package trabica.model

enum StateTransitionReason {
  case HigherTermDiscovered
  case NoHeartbeat
  case ElectedLeader
  case ElectionStarted
  case ConfigurationChanged
}
