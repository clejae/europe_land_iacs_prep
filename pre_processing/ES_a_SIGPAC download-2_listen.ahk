; Set a hotkey to trigger the automation (e.g., Ctrl+Alt+u)
^!u::
Loop
{
    Send, {Tab 4} ; (starts before at 'download' button)
    Sleep, 1000	
    Send, {Down} ;pick next municipio
    Sleep, 1000			
    Send, {Tab}
    Sleep, 1000
    Send, {Down 2} ;choose 2022 gpkg as file
    Sleep, 1000
    Send, {Tab 3}
    Sleep, 1000
    Send, {Enter}
    Sleep, 50000 ;wait for download
    Sleep, 50	
}

return

; Set a hotkey to stop the script (e.g., Ctrl+Alt+i)
^!i::ExitApp
