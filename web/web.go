package web

import "embed"

//go:embed index.html style.css app.js charts.js
var Assets embed.FS
