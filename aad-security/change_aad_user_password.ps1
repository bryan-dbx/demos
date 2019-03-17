Connect-MsolService # connect as brysmi@brysmidbx.onmicrosoft.com

$login = "user00@brysmidbx.onmicrosoft.com"
$newpassword = "sparkDBX001"

for ($i=0; $i -le 30; $i++){
	$n = "$i".PadLeft(2, "0")
	$login = "user$n@brysmidbx.onmicrosoft.com"
	Set-MsolUserPassword -UserPrincipalName $login -NewPassword $newpassword
	#write-host $login
	}