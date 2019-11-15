#!/bin/bash
: '
  Copyright 2019 ABSA Group Limited

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
'
export N_PREFIX=$HOME/.n
npm install n --no-save --no-color
node_modules/.bin/n 10.16.3
$HOME/.n/bin/node node_modules/.bin/ng build --prod
