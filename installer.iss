; Inno Setup script for Merge & Resample Service

[Setup]
AppId={{YOUR-UNIQUE-APP-ID}}
AppName=Merge & Resample Service
AppVersion=1.0
AppPublisher=Your Name
DefaultDirName={autopf}\MergeResampleService
DefaultGroupName=Merge & Resample Service
DisableProgramGroupPage=yes
OutputBaseFilename=Setup-MergeResampleService
Compression=lzma
SolidCompression=yes
WizardStyle=modern
UninstallDisplayIcon={app}\MergeResampleApp.exe
; Optional: Use your icon.ico for the installer and uninstaller
SetupIconFile=icon.ico

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: checkablealone

[Files]
; This tells Inno Setup to grab everything PyInstaller creates in the 'dist' folder
Source: "dist\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{group}\Merge & Resample Service"; Filename: "{app}\MergeResampleApp.exe"
Name: "{autodesktop}\Merge & Resample Service"; Filename: "{app}\MergeResampleApp.exe"; Tasks: desktopicon

[Run]
; Launch the application after the installation finishes
Filename: "{app}\MergeResampleApp.exe"; Description: "{cm:LaunchProgram,Merge & Resample Service}"; Flags: nowait postinstall skipifsilent