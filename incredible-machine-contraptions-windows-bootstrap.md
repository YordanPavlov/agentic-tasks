# Bootstrap: Run "Return of the Incredible Machine: Contraptions" fullscreen on Windows 11

**Goal:** Get the 2000 Sierra game *Return of the Incredible Machine: Contraptions* running **fullscreen** on a Windows 11 (dual-boot) machine, using the disk images investigated on the Linux side.

**Status:** Game runs fine under Linux/Wine but only in a small corner (see pain points below). Native Windows 11 is expected to give clean fullscreen. This doc is the handoff to make that attempt.

---

## 1. What the game is

- **Title:** Return of the Incredible Machine: Contraptions (Sierra On-Line, product date 2000-07-03, v1.0.0.0, CD sub S7096610).
- **Type:** 32-bit Windows game using **exclusive-fullscreen DirectDraw**, native mode **640×480** (likely; the game selects its own mode).
- **Self-contained:** The entire runnable game lives in a single `Files\` folder on the disc: `Contraptions.exe` + data blobs `*.tbv` (Interfaces, Intro, Levels, Music, Parts, Sounds, System, Voice) + `Roboex32.dll` + `*.mid` music + help files. No CD is required at runtime (it ran from a hard-disk copy with no disc mounted).
- The discs are **hybrid Mac+PC** (Apple Partition Map + HFS *and* an ISO 9660/Joliet PC filesystem). Only the PC side matters for Windows.

## 2. Which image to use (IMPORTANT)

Several dumps exist in `~/Downloads/incredible_machine_the_return/`. Use the right one:

| File | Verdict | Notes |
|------|---------|-------|
| **`Contraptions_pc.iso`** (445 MB) | ✅ **USE THIS** | Clean 2048-byte ISO 9660 + Joliet, derived from the complete/verified dump. **Windows 11 mounts it natively** (double-click → drive letter). Every game file verified readable. |
| `Contraptions.bin` + `Contraptions.cue` (from `Disc.rar`, 511 MB) | ✅ pristine source | The original complete dump `Contraptions_pc.iso` was made from. `.bin/.cue` needs a mounting tool on Windows (e.g. **WinCDEmu**, free). Use only if the ISO misbehaves. |
| `The Incredible Machine - Contraptions (January 22 2002).iso` (252 MB) | ❌ AVOID | **Incomplete dump** — the `.tbv` data files give I/O errors / read as 0 bytes. |
| `return_incredible_machine_contraptions.iso` (445 MB) | ❌ AVOID (Mac) | Mac HFS only; no PC executables. |
| `contraptions/CONTRAPTIONS.BIN` (275 MB, MODE2/2352) | ⚠️ untested | PC-only, smaller; not verified. Only a last resort. |

**Move `Contraptions_pc.iso` to the Windows partition.**

> Even simpler fallback: the game is fully self-contained, so you can instead copy the already-extracted folder `~/Downloads/incredible_machine_the_return/game/` to Windows and run `Contraptions.exe` directly — no ISO, no installer, no CD.

## 3. Why fullscreen failed on Linux (the pain points — Linux-specific)

These are **not** game bugs; they are old-DirectDraw-vs-modern-Linux-display issues:

1. **Wine on Wayland can't change the physical resolution.** The game requests an exclusive-fullscreen 640×480 mode switch; Wayland/Wine can't apply it, so Wine "fake-fullscreens": it blits the 640×480 image into the **top-left corner** of the 3840×2160 screen, rest black.
2. **gamescope crashes the game.** Forcing gamescope's nested output to a small resolution (640×480 or 800×600) makes the game's DirectDraw **primary-surface allocation fail → NULL-pointer write → crash** ("encountered a serious problem"). Reproduced deterministically at fault address `0045C722` writing `0x0000054A`, with both the Wayland and X11 Wine drivers, and with/without a Wine virtual desktop.
3. Wine virtual desktop (`explorer /desktop=`) avoids the crash but the game doesn't paint into it (blue background only).

**Conclusion:** the blocker is the Linux display stack doing no real modeset + gamescope's DirectDraw incompatibility. On native Windows the display mode switch actually happens, so these don't apply.

## 4. Expected behaviour on Windows 11 (analysis)

- **Will it run?** Almost certainly — Windows 11 still supports DirectDraw (emulated over Direct3D/DWM). *Confidence ~0.9.*
- **Fullscreen?** Very likely, and cleanly, because the real 640×480 mode switch succeeds and the GPU/monitor scales it. *Confidence ~0.75.*
- **The one likely gotcha (Windows analog of the "corner" problem):** GPU **display-scaling** setting. If the GPU control panel is set to *Centered / No scaling*, 640×480 shows as a small centered image with black borders. Set it to *Full-screen* or *Maintain aspect ratio* to fill the screen.
- **Copy protection:** Windows 11 removed the SafeDisc driver, so a protected exe could fail — **but** this game ran from hard disk with no CD under Wine, strongly implying it's unprotected. Low risk. *Confidence ~0.7.*

## 5. Windows 11 bootstrap steps

1. **Copy** `Contraptions_pc.iso` to the Windows drive.
2. **Mount it:** right-click → *Mount* (or double-click). It appears as a CD drive letter.
3. **Install or run directly:**
   - Run `Setup.exe` from the mounted disc to install normally, **or**
   - Skip the installer and run `Files\Contraptions.exe` directly (game is self-contained). The autorun command the disc uses is `files\Contraptions.exe -a`.
4. **If it launches but the image is small/centered (not fullscreen):** open the GPU control panel and set display scaling to **Full-screen** (or *Maintain aspect ratio*):
   - Intel: Intel Graphics Command Center → Display → Scaling → *Full Screen*.
   - NVIDIA: Control Panel → *Adjust desktop size and position* → Scaling → *Full-screen* (+ "Perform scaling on: GPU").
   - AMD: Adrenalin → Display → *GPU Scaling* On → *Full panel* / *Preserve aspect ratio*.
5. **If the game refuses to launch or crashes on the mode switch:** right-click `Contraptions.exe` → Properties → **Compatibility** → try *Windows XP (SP3)* mode and/or *Reduced color mode (16-bit)* and *Disable fullscreen optimizations*.
6. **Fallbacks if the ISO won't mount or data seems missing:**
   - Mount the pristine `Contraptions.bin`/`.cue` via **WinCDEmu** instead.
   - Or just copy the `game\` folder and run `Contraptions.exe`.

## 6. Fallback still on Linux (not yet done)

If Windows is not an option, the remaining Linux path is **dgVoodoo2** — a `ddraw.dll` wrapper dropped next to `Contraptions.exe` that renders DirectDraw through Direct3D and scales to fullscreen without a real modeset (so it avoids both pain points above). Set `WINEDLLOVERRIDES="ddraw=n,b"` and configure dgVoodoo for stretched/aspect scaling. This was agreed but not yet downloaded/installed.

## 7. Reference: Linux artifacts already created

Under `~/Downloads/incredible_machine_the_return/`:
- `game/` — extracted, runnable game files (the `Files\` folder contents).
- `Contraptions_pc.iso` — the clean PC ISO (move this to Windows).
- `play.sh` — plain Wine launcher (works; renders in corner).
- `play-fs.sh` — gamescope+virtual-desktop launcher (crashes this game; kept for reference).
- Wine prefix: `~/.wine-contraptions` (Wine 10).
