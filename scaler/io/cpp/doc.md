async binder:
  - has a ROUTER socket
    - bind sync
  - callback that takes identity and msg
  - keeps statistics on num recv and sent
  
  - methods:
    - async send msg to an identity
    - async recv a message from a peer w/ identity
