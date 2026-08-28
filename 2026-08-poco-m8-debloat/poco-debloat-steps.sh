# Make sure device is shown. On first call popup would show on phone
adb devices

#List of devices attached
#bf0ec90b	device

adb shell pm uninstall -k --user 0 com.miui.analytics
adb shell pm uninstall -k --user 0 com.xiaomi.joyose
adb shell pm uninstall -k --user 0 com.xiaomi.mipicks
adb shell pm uninstall -k --user 0 com.mi.globalbrowser
adb shell pm uninstall -k --user 0 com.miui.player
adb shell pm uninstall -k --user 0 com.miui.videoplayer
adb shell pm uninstall -k --user 0 com.miui.android.fashiongallery
adb shell pm uninstall -k --user 0 com.android.thememanager
adb shell pm uninstall -k --user 0 com.miui.bugreport
adb shell pm uninstall -k --user 0 com.xiaomi.payment
adb shell pm uninstall -k --user 0 com.xiaomi.micloud.sdk
adb shell pm uninstall -k --user 0 com.miui.cleaner
adb shell pm uninstall -k --user 0 cn.wps.xiaomi.abroad.lite
adb shell pm uninstall -k --user 0 com.miui.yellowpage
adb shell pm uninstall -k --user 0 com.duokan.phone.remotecontroller
adb shell pm uninstall -k --user 0 com.google.android.apps.subscriptions.red
adb shell pm uninstall -k --user 0 com.google.android.apps.bard
# For the one below - needs to sign in with Mi Account and then the grayed out USB debugging (Security settings)
# becomes active in Settings -> Developer options.
adb shell pm disable-user --user 0 com.xiaomi.midrop
