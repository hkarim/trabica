package trabica.store

import trabica.model.{Index, Term}

enum AppendResult {
  // unsuccessful results
  case IndexExistsWithTermConflict(storeTerm: Term, incomingTerm: Term)
  case HigherTermExists(storeTerm: Term, incomingTerm: Term)
  case NonMonotonicIndex(storeIndex: Index, incomingIndex: Index)
  // successful results
  case IndexExists
  case Appended
}
