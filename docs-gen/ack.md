## `lagotto ack`

Acknowledge an --until-landed watch.

An --until-landed watch has no TTL. Instead it pauses itself unless you
acknowledge it: its deadline is (2+1) × its --remind interval (21 days by
default) from creation or the last acknowledgement. ack moves the deadline a
full window ahead from now. A paused watch (status expired, reason
unacknowledged) is resumed and hunts again.

Only ack and extend count as an acknowledgement; list and status do not.

```
lagotto ack <watch-id>
```

