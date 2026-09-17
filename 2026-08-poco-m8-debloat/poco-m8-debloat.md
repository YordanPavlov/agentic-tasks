# Poco M8 debloat (re-run after OS upgrade)

## Goal

Re-debloat the Poco M8 after a phone OS upgrade resurrected packages that were
removed in the original debloat pass (April 2026, documented in
`~/src/santiment-cheatsheets/poco-m8-phone/`). This task directory is now the
single place for this work; the cheatsheet files were copied here.

## Files

- `debloat-poco.sh` — standalone, re-runnable debloat script (the current
  tool). Checks the device, snapshots the package list to
  `packages-<timestamp>.txt`, uninstalls bloat for user 0 only, disables
  Mi Drop. No `-k` flag: app data is removed too. Reversible via
  `adb shell pm install-existing <package>`.
- `poco-debloat-steps.sh` — original one-liner list from the first pass
  (reference only, superseded by `debloat-poco.sh`).
- `diagnostics-tools.txt` — hidden Poco diagnostics menu (`*#*#6484#*#*` in
  the dialer).

## Notes

- Package snapshots taken by the script can be diffed between runs to spot
  new bloat the upgrade introduced.
- Mi Drop (`com.xiaomi.midrop`) is disabled, not uninstalled: without it the
  "USB debugging (Security settings)" toggle stays grayed out unless signed
  in to a Mi Account.
- Animation settings (2026-09-17 lag session): the three standard animation
  scales are off in Developer options; the MIUI-specific recents/transition
  animation (spring-based, ignores those scales) is sped up 5x via
  `adb shell settings put global transition_animation_duration_ratio 0.2`.
  Writing global settings needs "USB debugging (Security settings)", which
  requires a Xiaomi Account sign-in (done, old account). An OS upgrade may
  reset any of these (the 2026-08 upgrade reset plain USB debugging, stored
  the same way) — re-check after upgrades.

## Session log

### 2026-08-28

- Reviewed the original plan; still valid. Agreed changes: drop `-k` (remove
  app data too) and replace the blind uninstall lines with a loop that checks
  what is installed, skips what is gone, and reports what it did.
- Created this task directory, copied the cheatsheet files, wrote
  `debloat-poco.sh`.
- Phone not yet visible: initially USB debugging was off (reset by the
  upgrade); after enabling it the device still does not appear in `adb
  devices` or `lsusb`, so the host does not see it at USB level — suspect a
  charge-only cable or bad port. Pending: different cable/port, then run the
  script.
- Cable swap fixed detection. First script run (`packages-20260828-125734.txt`):
  all 17 original packages were already gone — the upgrade did not resurrect
  them; only Mi Drop had been re-enabled and was disabled again.
- Snapshot review found new bloat from the upgrade; added 7 packages to the
  script: msa.global (ads), globalminusscreen, glgm, discover,
  thirdappassistant, barrage, miservice. Second run
  (`packages-20260828-125945.txt`) removed all 7 successfully. Phone is
  debloated; task complete.
- Home-screen search UI showed ads: that is `com.mi.appfinder` (App Finder).
  Removed it directly and added it to the script (24 → 25 packages).
  Confirmed after reboot: search bar gone. Session closed.

### 2026-08-31

- Third iteration. Script run (`packages-20260831-135439.txt`): nothing
  resurrected, all 25 packages still gone; Mi Drop re-disabled (the script
  re-disables it every run).
- User-driven cleanup of preinstalled apps: removed Android Auto
  (`com.google.android.projection.gearhead`), the silent Facebook/Amazon
  installer stubs (`com.facebook.appmanager`/`services`/`system`,
  `com.amazon.appmanager` — Facebook Messenger itself is kept and unaffected),
  Link to Windows (`com.microsoft.appmanager` + two service packages), and
  Mi Cloud (`com.miui.cloudbackup`/`cloudservice`/`micloudsync` — no Mi
  Account in use). All 11 added to the script (25 → 36 packages).

### 2026-09-17 — lag investigation

- Perceived lags examined over adb: phone healthy (no throttling, battery
  saver off, RAM status normal). Gboard frame stats measured live: Latin↔
  Cyrillic switch costs 150–300ms of layout/language-model rebuild —
  inherent Gboard behavior, unrelated to the debloat.
- Removed `com.xiaomi.aiservice` + `com.xiaomi.aicr` (~700MB RAM combined);
  added to the script (36 → 38). `com.xiaomi.aiasst.vision` left in place.
- Slow keyboard pop-up and window transitions fixed via animation settings —
  see Notes. `miui_home_animation_rate` (system table) was tried and had no
  effect; reverted to 1.
- MIUI blocks `settings put` from adb: system table unlocked via
  `adb shell appops set com.android.shell WRITE_SETTINGS allow`; global
  table only via the "USB debugging (Security settings)" toggle, for which
  the user signed into their existing Xiaomi Account.
