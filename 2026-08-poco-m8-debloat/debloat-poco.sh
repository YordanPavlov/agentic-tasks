#!/usr/bin/env bash
# Debloat a Poco/Xiaomi phone over adb, for user 0 only (reversible).
#
# Standalone: needs only adb and a single connected, authorized device.
# Safe to re-run any time (e.g. after an OS upgrade resurrects packages) —
# it checks what is installed first and only acts on what is present.
#
# Restore a package later with:
#   adb shell pm install-existing <package>
#
# Snapshots of the full package list are written to the current directory
# as packages-<timestamp>.txt, so runs can be diffed for new bloat.
set -euo pipefail

UNINSTALL_PACKAGES=(
  com.miui.analytics
  com.xiaomi.joyose
  com.xiaomi.mipicks
  com.mi.globalbrowser
  com.miui.player
  com.miui.videoplayer
  com.miui.android.fashiongallery
  com.android.thememanager
  com.miui.bugreport
  com.xiaomi.payment
  com.xiaomi.micloud.sdk
  com.miui.cleaner
  cn.wps.xiaomi.abroad.lite
  com.miui.yellowpage
  com.duokan.phone.remotecontroller
  com.google.android.apps.subscriptions.red
  com.google.android.apps.bard
  # Added 2026-08 after the OS upgrade introduced new bloat:
  com.miui.msa.global
  com.mi.globalminusscreen
  com.xiaomi.glgm
  com.xiaomi.discover
  com.miui.thirdappassistant
  com.xiaomi.barrage
  com.miui.miservice
  com.mi.appfinder
  # Added 2026-08-31, third iteration:
  com.google.android.projection.gearhead  # Android Auto
  com.facebook.appmanager                 # silent Facebook installer/updater
  com.facebook.services
  com.facebook.system
  com.amazon.appmanager                   # silent Amazon installer/updater
  com.microsoft.appmanager                # Link to Windows
  com.microsoft.deviceintegrationservice
  com.microsoftsdk.crossdeviceservicebroker
  com.miui.cloudbackup                    # Mi Cloud (unused, no Mi Account)
  com.miui.cloudservice
  com.miui.micloudsync
)

# Only disabled, not uninstalled: with Mi Drop removed the "USB debugging
# (Security settings)" toggle in Developer options stays grayed out unless
# signed in to a Mi Account.
DISABLE_PACKAGES=(
  com.xiaomi.midrop
)

devices=$(adb devices | tr -d '\r' | awk 'NR>1 && $2=="device" {print $1}')
count=$(echo -n "$devices" | grep -c . || true)
if [[ "$count" -ne 1 ]]; then
  echo "ERROR: expected exactly 1 authorized device, found $count." >&2
  adb devices >&2
  exit 1
fi
echo "Device: $devices"

snapshot="packages-$(date +%Y%m%d-%H%M%S).txt"
adb shell pm list packages --user 0 | tr -d '\r' | sed 's/^package://' | sort > "$snapshot"
echo "Package snapshot saved to $snapshot"

installed() {
  grep -qx "$1" "$snapshot"
}

removed=0 skipped=0
for pkg in "${UNINSTALL_PACKAGES[@]}"; do
  if installed "$pkg"; then
    echo "Uninstalling $pkg ..."
    adb shell pm uninstall --user 0 "$pkg"
    removed=$((removed + 1))
  else
    echo "Skipping $pkg (not installed for user 0)"
    skipped=$((skipped + 1))
  fi
done

for pkg in "${DISABLE_PACKAGES[@]}"; do
  if installed "$pkg"; then
    echo "Disabling $pkg ..."
    adb shell pm disable-user --user 0 "$pkg"
  else
    echo "Skipping $pkg (not installed for user 0)"
  fi
done

echo "Done: $removed removed, $skipped already gone."
