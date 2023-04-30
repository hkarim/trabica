package trabica.model

enum StateTransitionReason {
  case HigherTermDiscovered
  case NoHeartbeat
  case ElectedLeader
  case JoinAccepted
  case ConfigurationChanged
}
