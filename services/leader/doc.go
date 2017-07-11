/*
Package leader provides functionality for etcd-backed leader elections.

The following diagram illustrates the states of an election:

                         FOLLOWER<---------------+
                            +                    ^
                            |                    |
          Campaign() blocks +-------------------->
                            |     Resign()       |
                            |                    |
                            v                    |
                   +--------+                    |
                   |        |                    |
                   |        | Campaign() OK      |
                   |        |                    |
                   |        |                    |
   Campaign() Err  |        v      Resign()      |
                   |      LEADER+--------------->+
                   |        +                    ^
                   |        | Lose session       |
                   |        |                    |
                   |        v                    |
                   +----->ERROR+-----------------+
                                Campaign() again

An election starts in FOLLOWER state when a call to Campaign() is first made.
The underlying call to etcd will block until the client is either (a) elected
(in which case the election will be in LEADER state) or (b) an error is
encountered (in which case election will be in ERROR state). If an election is
in LEADER state but the session expires in the background it will transition to
ERROR state (and Campaign() will need to be called again to progress). If an
election is in LEADER state and the user calls Resign() it will transition back
to FOLLOWER state. Finally, if an election is in FOLLOWER state and a blocking
call to Campaign() is ongoing and the user calls Resign(), the campaign will be
cancelled and the election back in FOLLOWER state.

Callers of Campaign() MUST consume the returned channel until it is closed or
risk goroutine leaks.
*/
package leader
