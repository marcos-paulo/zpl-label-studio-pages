var Gw=Object.defineProperty,Ww=Object.defineProperties;var qw=Object.getOwnPropertyDescriptors;var $v=Object.getOwnPropertySymbols;var Yw=Object.prototype.hasOwnProperty,Zw=Object.prototype.propertyIsEnumerable;var Gv=(t,n,e)=>n in t?Gw(t,n,{enumerable:!0,configurable:!0,writable:!0,value:e}):t[n]=e,v=(t,n)=>{for(var e in n||={})Yw.call(n,e)&&Gv(t,e,n[e]);if($v)for(var e of $v(n))Zw.call(n,e)&&Gv(t,e,n[e]);return t},k=(t,n)=>Ww(t,qw(n));var Mt=null,$l=!1,pr=1,Xw=null,et=Symbol("SIGNAL");function K(t){let n=Mt;return Mt=t,n}function Yl(){return Mt}var gr={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function vr(t){if($l)throw new Error("");if(Mt===null)return;Mt.consumerOnSignalRead(t);let n=Mt.producersTail;if(n!==void 0&&n.producer===t)return;let e,i=Mt.recomputing;if(i&&(e=n!==void 0?n.nextProducer:Mt.producers,e!==void 0&&e.producer===t)){Mt.producersTail=e,e.lastReadVersion=t.version,e.knownValidAtEpoch=pr;return}let r=t.consumersTail;if(r!==void 0&&r.consumer===Mt&&(!i||r.knownValidAtEpoch===pr))return;let o=Do(Mt),s={producer:t,consumer:Mt,nextProducer:e,prevConsumer:void 0,knownValidAtEpoch:pr,lastReadVersion:t.version,nextConsumer:void 0};Mt.producersTail=s,n!==void 0?n.nextProducer=s:Mt.producers=s,o&&Zv(t,s)}function Wv(){pr++}function Zl(t){if(!(Do(t)&&!t.dirty)&&!(!t.dirty&&t.lastCleanEpoch===pr)){if(!t.producerMustRecompute(t)&&!Co(t)){ql(t);return}t.producerRecomputeValue(t),ql(t)}}function Uf(t){if(t.consumers===void 0)return;let n=$l;$l=!0;try{for(let e=t.consumers;e!==void 0;e=e.nextConsumer){let i=e.consumer;i.dirty||Kw(i)}}finally{$l=n}}function zf(){return Mt?.consumerAllowSignalWrites!==!1}function Kw(t){t.dirty=!0,Uf(t),t.consumerMarkedDirty?.(t)}function ql(t){t.dirty=!1,t.lastCleanEpoch=pr}function Ii(t){return t&&qv(t),K(t)}function qv(t){if(t.producersTail?.knownValidAtEpoch===pr){let n=t.producers;for(;n!==void 0;)n.knownValidAtEpoch=null,n=n.nextProducer}t.producersTail=void 0,t.recomputing=!0}function br(t,n){K(n),t&&Yv(t)}function Yv(t){t.recomputing=!1;let n=t.producersTail,e=n!==void 0?n.nextProducer:t.producers;if(e!==void 0){if(Do(t))do e=$f(e);while(e!==void 0);n!==void 0?n.nextProducer=void 0:t.producers=void 0}}function Co(t){for(let n=t.producers;n!==void 0;n=n.nextProducer){let e=n.producer,i=n.lastReadVersion;if(i!==e.version||(Zl(e),i!==e.version))return!0}return!1}function Mi(t){if(Do(t)){let n=t.producers;for(;n!==void 0;)n=$f(n)}t.producers=void 0,t.producersTail=void 0,t.consumers=void 0,t.consumersTail=void 0}function Zv(t,n){let e=t.consumersTail,i=Do(t);if(e!==void 0?(n.nextConsumer=e.nextConsumer,e.nextConsumer=n):(n.nextConsumer=void 0,t.consumers=n),n.prevConsumer=e,t.consumersTail=n,!i)for(let r=t.producers;r!==void 0;r=r.nextProducer)Zv(r.producer,r)}function $f(t){let n=t.producer,e=t.nextProducer,i=t.nextConsumer,r=t.prevConsumer;if(t.nextConsumer=void 0,t.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:n.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(n.consumers=i,!Do(n)){let o=n.producers;for(;o!==void 0;)o=$f(o)}return e}function Do(t){return t.consumerIsAlwaysLive||t.consumers!==void 0}function Xl(t){Xw?.(t)}function Kl(t,n){return Object.is(t,n)}function Hs(t,n){let e=Object.create(Qw);e.computation=t,n!==void 0&&(e.equal=n);let i=()=>{if(Zl(e),vr(e),e.value===js)throw e.error;return e.value};return i[et]=e,Xl(e),i}var Gl=Symbol("UNSET"),Wl=Symbol("COMPUTING"),js=Symbol("ERRORED"),Qw=k(v({},gr),{value:Gl,dirty:!0,error:null,equal:Kl,kind:"computed",producerMustRecompute(t){return t.value===Gl||t.value===Wl},producerRecomputeValue(t){if(t.value===Wl)throw new Error("");let n=t.value;t.value=Wl;let e=Ii(t),i,r=!1;try{i=t.computation(),K(null),r=n!==Gl&&n!==js&&i!==js&&t.equal(n,i)}catch(o){i=js,t.error=o}finally{br(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function Jw(){throw new Error}var Xv=Jw;function Kv(t){Xv(t)}function Gf(t){Xv=t}var ex=null;function Wf(t,n){let e=Object.create(Eo);e.value=t,n!==void 0&&(e.equal=n);let i=()=>Qv(e);return i[et]=e,Xl(e),[i,s=>_r(e,s),s=>qf(e,s)]}function Qv(t){return vr(t),t.value}function _r(t,n){zf()||Kv(t),t.equal(t.value,n)||(t.value=n,tx(t))}function qf(t,n){zf()||Kv(t),_r(t,n(t.value))}var Eo=k(v({},gr),{equal:Kl,value:void 0,kind:"signal"});function tx(t){t.version++,Wv(),Uf(t),ex?.(t)}var Yf=k(v({},gr),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function Zf(t){if(t.dirty=!1,t.version>0&&!Co(t))return;t.version++;let n=Ii(t);try{t.cleanup(),t.fn()}finally{br(t,n)}}var Xf;function Ql(){return Xf}function kn(t){let n=Xf;return Xf=t,n}var Jv=Symbol("NotFound");function wo(t){return t===Jv||t?.name==="\u0275NotFound"}function eb(t){let n=K(null);try{return t()}finally{K(n)}}function ae(t){return typeof t=="function"}function xo(t){let e=t(i=>{Error.call(i),i.stack=new Error().stack});return e.prototype=Object.create(Error.prototype),e.prototype.constructor=e,e}var Jl=xo(t=>function(e){t(this),this.message=e?`${e.length} errors occurred during unsubscription:
${e.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=e});function yr(t,n){if(t){let e=t.indexOf(n);0<=e&&t.splice(e,1)}}var pe=class t{constructor(n){this.initialTeardown=n,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let n;if(!this.closed){this.closed=!0;let{_parentage:e}=this;if(e)if(this._parentage=null,Array.isArray(e))for(let o of e)o.remove(this);else e.remove(this);let{initialTeardown:i}=this;if(ae(i))try{i()}catch(o){n=o instanceof Jl?o.errors:[o]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let o of r)try{tb(o)}catch(s){n=n??[],s instanceof Jl?n=[...n,...s.errors]:n.push(s)}}if(n)throw new Jl(n)}}add(n){var e;if(n&&n!==this)if(this.closed)tb(n);else{if(n instanceof t){if(n.closed||n._hasParent(this))return;n._addParent(this)}(this._finalizers=(e=this._finalizers)!==null&&e!==void 0?e:[]).push(n)}}_hasParent(n){let{_parentage:e}=this;return e===n||Array.isArray(e)&&e.includes(n)}_addParent(n){let{_parentage:e}=this;this._parentage=Array.isArray(e)?(e.push(n),e):e?[e,n]:n}_removeParent(n){let{_parentage:e}=this;e===n?this._parentage=null:Array.isArray(e)&&yr(e,n)}remove(n){let{_finalizers:e}=this;e&&yr(e,n),n instanceof t&&n._removeParent(this)}};pe.EMPTY=(()=>{let t=new pe;return t.closed=!0,t})();var Kf=pe.EMPTY;function ec(t){return t instanceof pe||t&&"closed"in t&&ae(t.remove)&&ae(t.add)&&ae(t.unsubscribe)}function tb(t){ae(t)?t():t.unsubscribe()}var ln={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var Io={setTimeout(t,n,...e){let{delegate:i}=Io;return i?.setTimeout?i.setTimeout(t,n,...e):setTimeout(t,n,...e)},clearTimeout(t){let{delegate:n}=Io;return(n?.clearTimeout||clearTimeout)(t)},delegate:void 0};function tc(t){Io.setTimeout(()=>{let{onUnhandledError:n}=ln;if(n)n(t);else throw t})}function Us(){}var nb=Qf("C",void 0,void 0);function ib(t){return Qf("E",void 0,t)}function rb(t){return Qf("N",t,void 0)}function Qf(t,n,e){return{kind:t,value:n,error:e}}var Sr=null;function Mo(t){if(ln.useDeprecatedSynchronousErrorHandling){let n=!Sr;if(n&&(Sr={errorThrown:!1,error:null}),t(),n){let{errorThrown:e,error:i}=Sr;if(Sr=null,e)throw i}}else t()}function ob(t){ln.useDeprecatedSynchronousErrorHandling&&Sr&&(Sr.errorThrown=!0,Sr.error=t)}var Cr=class extends pe{constructor(n){super(),this.isStopped=!1,n?(this.destination=n,ec(n)&&n.add(this)):this.destination=rx}static create(n,e,i){return new Xn(n,e,i)}next(n){this.isStopped?eh(rb(n),this):this._next(n)}error(n){this.isStopped?eh(ib(n),this):(this.isStopped=!0,this._error(n))}complete(){this.isStopped?eh(nb,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(n){this.destination.next(n)}_error(n){try{this.destination.error(n)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},nx=Function.prototype.bind;function Jf(t,n){return nx.call(t,n)}var th=class{constructor(n){this.partialObserver=n}next(n){let{partialObserver:e}=this;if(e.next)try{e.next(n)}catch(i){nc(i)}}error(n){let{partialObserver:e}=this;if(e.error)try{e.error(n)}catch(i){nc(i)}else nc(n)}complete(){let{partialObserver:n}=this;if(n.complete)try{n.complete()}catch(e){nc(e)}}},Xn=class extends Cr{constructor(n,e,i){super();let r;if(ae(n)||!n)r={next:n??void 0,error:e??void 0,complete:i??void 0};else{let o;this&&ln.useDeprecatedNextContext?(o=Object.create(n),o.unsubscribe=()=>this.unsubscribe(),r={next:n.next&&Jf(n.next,o),error:n.error&&Jf(n.error,o),complete:n.complete&&Jf(n.complete,o)}):r=n}this.destination=new th(r)}};function nc(t){ln.useDeprecatedSynchronousErrorHandling?ob(t):tc(t)}function ix(t){throw t}function eh(t,n){let{onStoppedNotification:e}=ln;e&&Io.setTimeout(()=>e(t,n))}var rx={closed:!0,next:Us,error:ix,complete:Us};var To=typeof Symbol=="function"&&Symbol.observable||"@@observable";function Lt(t){return t}function ic(...t){return nh(t)}function nh(t){return t.length===0?Lt:t.length===1?t[0]:function(e){return t.reduce((i,r)=>r(i),e)}}var ne=class t{constructor(n){n&&(this._subscribe=n)}lift(n){let e=new t;return e.source=this,e.operator=n,e}subscribe(n,e,i){let r=sx(n)?n:new Xn(n,e,i);return Mo(()=>{let{operator:o,source:s}=this;r.add(o?o.call(r,s):s?this._subscribe(r):this._trySubscribe(r))}),r}_trySubscribe(n){try{return this._subscribe(n)}catch(e){n.error(e)}}forEach(n,e){return e=sb(e),new e((i,r)=>{let o=new Xn({next:s=>{try{n(s)}catch(a){r(a),o.unsubscribe()}},error:r,complete:i});this.subscribe(o)})}_subscribe(n){var e;return(e=this.source)===null||e===void 0?void 0:e.subscribe(n)}[To](){return this}pipe(...n){return nh(n)(this)}toPromise(n){return n=sb(n),new n((e,i)=>{let r;this.subscribe(o=>r=o,o=>i(o),()=>e(r))})}};ne.create=t=>new ne(t);function sb(t){var n;return(n=t??ln.Promise)!==null&&n!==void 0?n:Promise}function ox(t){return t&&ae(t.next)&&ae(t.error)&&ae(t.complete)}function sx(t){return t&&t instanceof Cr||ox(t)&&ec(t)}function ax(t){return ae(t?.lift)}function ue(t){return n=>{if(ax(n))return n.lift(function(e){try{return t(e,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function fe(t,n,e,i,r){return new ih(t,n,e,i,r)}var ih=class extends Cr{constructor(n,e,i,r,o,s){super(n),this.onFinalize=o,this.shouldUnsubscribe=s,this._next=e?function(a){try{e(a)}catch(l){n.error(l)}}:super._next,this._error=r?function(a){try{r(a)}catch(l){n.error(l)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(a){n.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var n;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:e}=this;super.unsubscribe(),!e&&((n=this.onFinalize)===null||n===void 0||n.call(this))}}};var ab=xo(t=>function(){t(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var E=class extends ne{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(n){let e=new rc(this,this);return e.operator=n,e}_throwIfClosed(){if(this.closed)throw new ab}next(n){Mo(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let e of this.currentObservers)e.next(n)}})}error(n){Mo(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=n;let{observers:e}=this;for(;e.length;)e.shift().error(n)}})}complete(){Mo(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:n}=this;for(;n.length;)n.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var n;return((n=this.observers)===null||n===void 0?void 0:n.length)>0}_trySubscribe(n){return this._throwIfClosed(),super._trySubscribe(n)}_subscribe(n){return this._throwIfClosed(),this._checkFinalizedStatuses(n),this._innerSubscribe(n)}_innerSubscribe(n){let{hasError:e,isStopped:i,observers:r}=this;return e||i?Kf:(this.currentObservers=null,r.push(n),new pe(()=>{this.currentObservers=null,yr(r,n)}))}_checkFinalizedStatuses(n){let{hasError:e,thrownError:i,isStopped:r}=this;e?n.error(i):r&&n.complete()}asObservable(){let n=new ne;return n.source=this,n}};E.create=(t,n)=>new rc(t,n);var rc=class extends E{constructor(n,e){super(),this.destination=n,this.source=e}next(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.next)===null||i===void 0||i.call(e,n)}error(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.error)===null||i===void 0||i.call(e,n)}complete(){var n,e;(e=(n=this.destination)===null||n===void 0?void 0:n.complete)===null||e===void 0||e.call(n)}_subscribe(n){var e,i;return(i=(e=this.source)===null||e===void 0?void 0:e.subscribe(n))!==null&&i!==void 0?i:Kf}};var tt=class extends E{constructor(n){super(),this._value=n}get value(){return this.getValue()}_subscribe(n){let e=super._subscribe(n);return!e.closed&&n.next(this._value),e}getValue(){let{hasError:n,thrownError:e,_value:i}=this;if(n)throw e;return this._throwIfClosed(),i}next(n){super.next(this._value=n)}};var zs={now(){return(zs.delegate||Date).now()},delegate:void 0};var Ti=class extends E{constructor(n=1/0,e=1/0,i=zs){super(),this._bufferSize=n,this._windowTime=e,this._timestampProvider=i,this._buffer=[],this._infiniteTimeWindow=!0,this._infiniteTimeWindow=e===1/0,this._bufferSize=Math.max(1,n),this._windowTime=Math.max(1,e)}next(n){let{isStopped:e,_buffer:i,_infiniteTimeWindow:r,_timestampProvider:o,_windowTime:s}=this;e||(i.push(n),!r&&i.push(o.now()+s)),this._trimBuffer(),super.next(n)}_subscribe(n){this._throwIfClosed(),this._trimBuffer();let e=this._innerSubscribe(n),{_infiniteTimeWindow:i,_buffer:r}=this,o=r.slice();for(let s=0;s<o.length&&!n.closed;s+=i?1:2)n.next(o[s]);return this._checkFinalizedStatuses(n),e}_trimBuffer(){let{_bufferSize:n,_timestampProvider:e,_buffer:i,_infiniteTimeWindow:r}=this,o=(r?1:2)*n;if(n<1/0&&o<i.length&&i.splice(0,i.length-o),!r){let s=e.now(),a=0;for(let l=1;l<i.length&&i[l]<=s;l+=2)a=l;a&&i.splice(0,a+1)}}};var oc=class extends pe{constructor(n,e){super()}schedule(n,e=0){return this}};var $s={setInterval(t,n,...e){let{delegate:i}=$s;return i?.setInterval?i.setInterval(t,n,...e):setInterval(t,n,...e)},clearInterval(t){let{delegate:n}=$s;return(n?.clearInterval||clearInterval)(t)},delegate:void 0};var sc=class extends oc{constructor(n,e){super(n,e),this.scheduler=n,this.work=e,this.pending=!1}schedule(n,e=0){var i;if(this.closed)return this;this.state=n;let r=this.id,o=this.scheduler;return r!=null&&(this.id=this.recycleAsyncId(o,r,e)),this.pending=!0,this.delay=e,this.id=(i=this.id)!==null&&i!==void 0?i:this.requestAsyncId(o,this.id,e),this}requestAsyncId(n,e,i=0){return $s.setInterval(n.flush.bind(n,this),i)}recycleAsyncId(n,e,i=0){if(i!=null&&this.delay===i&&this.pending===!1)return e;e!=null&&$s.clearInterval(e)}execute(n,e){if(this.closed)return new Error("executing a cancelled action");this.pending=!1;let i=this._execute(n,e);if(i)return i;this.pending===!1&&this.id!=null&&(this.id=this.recycleAsyncId(this.scheduler,this.id,null))}_execute(n,e){let i=!1,r;try{this.work(n)}catch(o){i=!0,r=o||new Error("Scheduled action threw falsy error")}if(i)return this.unsubscribe(),r}unsubscribe(){if(!this.closed){let{id:n,scheduler:e}=this,{actions:i}=e;this.work=this.state=this.scheduler=null,this.pending=!1,yr(i,this),n!=null&&(this.id=this.recycleAsyncId(e,n,null)),this.delay=null,super.unsubscribe()}}};var rh=(()=>{class t{constructor(e,i=t.now){this.schedulerActionCtor=e,this.now=i}schedule(e,i=0,r){return new this.schedulerActionCtor(this,e).schedule(r,i)}}return t.now=zs.now,t})();var ac=class extends rh{constructor(n,e=rh.now){super(n,e),this.actions=[],this._active=!1}flush(n){let{actions:e}=this;if(this._active){e.push(n);return}let i;this._active=!0;do if(i=n.execute(n.state,n.delay))break;while(n=e.shift());if(this._active=!1,i){for(;n=e.shift();)n.unsubscribe();throw i}}};var Gs=new ac(sc),lb=Gs;var Ge=new ne(t=>t.complete());function lc(t){return t&&ae(t.schedule)}function oh(t){return t[t.length-1]}function cc(t){return ae(oh(t))?t.pop():void 0}function An(t){return lc(oh(t))?t.pop():void 0}function cb(t,n){return typeof oh(t)=="number"?t.pop():n}function ub(t,n,e,i){function r(o){return o instanceof e?o:new e(function(s){s(o)})}return new(e||(e=Promise))(function(o,s){function a(u){try{c(i.next(u))}catch(f){s(f)}}function l(u){try{c(i.throw(u))}catch(f){s(f)}}function c(u){u.done?o(u.value):r(u.value).then(a,l)}c((i=i.apply(t,n||[])).next())})}function db(t){var n=typeof Symbol=="function"&&Symbol.iterator,e=n&&t[n],i=0;if(e)return e.call(t);if(t&&typeof t.length=="number")return{next:function(){return t&&i>=t.length&&(t=void 0),{value:t&&t[i++],done:!t}}};throw new TypeError(n?"Object is not iterable.":"Symbol.iterator is not defined.")}function Dr(t){return this instanceof Dr?(this.v=t,this):new Dr(t)}function fb(t,n,e){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=e.apply(t,n||[]),r,o=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",s),r[Symbol.asyncIterator]=function(){return this},r;function s(p){return function(S){return Promise.resolve(S).then(p,f)}}function a(p,S){i[p]&&(r[p]=function(C){return new Promise(function($,F){o.push([p,C,$,F])>1||l(p,C)})},S&&(r[p]=S(r[p])))}function l(p,S){try{c(i[p](S))}catch(C){h(o[0][3],C)}}function c(p){p.value instanceof Dr?Promise.resolve(p.value.v).then(u,f):h(o[0][2],p)}function u(p){l("next",p)}function f(p){l("throw",p)}function h(p,S){p(S),o.shift(),o.length&&l(o[0][0],o[0][1])}}function hb(t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var n=t[Symbol.asyncIterator],e;return n?n.call(t):(t=typeof db=="function"?db(t):t[Symbol.iterator](),e={},i("next"),i("throw"),i("return"),e[Symbol.asyncIterator]=function(){return this},e);function i(o){e[o]=t[o]&&function(s){return new Promise(function(a,l){s=t[o](s),r(a,l,s.done,s.value)})}}function r(o,s,a,l){Promise.resolve(l).then(function(c){o({value:c,done:a})},s)}}var dc=(t=>t&&typeof t.length=="number"&&typeof t!="function");function uc(t){return ae(t?.then)}function fc(t){return ae(t[To])}function hc(t){return Symbol.asyncIterator&&ae(t?.[Symbol.asyncIterator])}function mc(t){return new TypeError(`You provided ${t!==null&&typeof t=="object"?"an invalid object":`'${t}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function lx(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var pc=lx();function gc(t){return ae(t?.[pc])}function vc(t){return fb(this,arguments,function*(){let e=t.getReader();try{for(;;){let{value:i,done:r}=yield Dr(e.read());if(r)return yield Dr(void 0);yield yield Dr(i)}}finally{e.releaseLock()}})}function bc(t){return ae(t?.getReader)}function ke(t){if(t instanceof ne)return t;if(t!=null){if(fc(t))return cx(t);if(dc(t))return dx(t);if(uc(t))return ux(t);if(hc(t))return mb(t);if(gc(t))return fx(t);if(bc(t))return hx(t)}throw mc(t)}function cx(t){return new ne(n=>{let e=t[To]();if(ae(e.subscribe))return e.subscribe(n);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function dx(t){return new ne(n=>{for(let e=0;e<t.length&&!n.closed;e++)n.next(t[e]);n.complete()})}function ux(t){return new ne(n=>{t.then(e=>{n.closed||(n.next(e),n.complete())},e=>n.error(e)).then(null,tc)})}function fx(t){return new ne(n=>{for(let e of t)if(n.next(e),n.closed)return;n.complete()})}function mb(t){return new ne(n=>{mx(t,n).catch(e=>n.error(e))})}function hx(t){return mb(vc(t))}function mx(t,n){var e,i,r,o;return ub(this,void 0,void 0,function*(){try{for(e=hb(t);i=yield e.next(),!i.done;){let s=i.value;if(n.next(s),n.closed)return}}catch(s){r={error:s}}finally{try{i&&!i.done&&(o=e.return)&&(yield o.call(e))}finally{if(r)throw r.error}}n.complete()})}function Rt(t,n,e,i=0,r=!1){let o=n.schedule(function(){e(),r?t.add(this.schedule(null,i)):this.unsubscribe()},i);if(t.add(o),!r)return o}function _c(t,n=0){return ue((e,i)=>{e.subscribe(fe(i,r=>Rt(i,t,()=>i.next(r),n),()=>Rt(i,t,()=>i.complete(),n),r=>Rt(i,t,()=>i.error(r),n)))})}function yc(t,n=0){return ue((e,i)=>{i.add(t.schedule(()=>e.subscribe(i),n))})}function pb(t,n){return ke(t).pipe(yc(n),_c(n))}function gb(t,n){return ke(t).pipe(yc(n),_c(n))}function vb(t,n){return new ne(e=>{let i=0;return n.schedule(function(){i===t.length?e.complete():(e.next(t[i++]),e.closed||this.schedule())})})}function bb(t,n){return new ne(e=>{let i;return Rt(e,n,()=>{i=t[pc](),Rt(e,n,()=>{let r,o;try{({value:r,done:o}=i.next())}catch(s){e.error(s);return}o?e.complete():e.next(r)},0,!0)}),()=>ae(i?.return)&&i.return()})}function Sc(t,n){if(!t)throw new Error("Iterable cannot be null");return new ne(e=>{Rt(e,n,()=>{let i=t[Symbol.asyncIterator]();Rt(e,n,()=>{i.next().then(r=>{r.done?e.complete():e.next(r.value)})},0,!0)})})}function _b(t,n){return Sc(vc(t),n)}function yb(t,n){if(t!=null){if(fc(t))return pb(t,n);if(dc(t))return vb(t,n);if(uc(t))return gb(t,n);if(hc(t))return Sc(t,n);if(gc(t))return bb(t,n);if(bc(t))return _b(t,n)}throw mc(t)}function Ve(t,n){return n?yb(t,n):ke(t)}function Y(...t){let n=An(t);return Ve(t,n)}function Ws(t,n){let e=ae(t)?t:()=>t,i=r=>r.error(e());return new ne(n?r=>n.schedule(i,0,r):i)}function qs(t){return!!t&&(t instanceof ne||ae(t.lift)&&ae(t.subscribe))}var Er=xo(t=>function(){t(this),this.name="EmptyError",this.message="no elements in sequence"});function Sb(t){return t instanceof Date&&!isNaN(t)}function oe(t,n){return ue((e,i)=>{let r=0;e.subscribe(fe(i,o=>{i.next(t.call(n,o,r++))}))})}var{isArray:px}=Array;function gx(t,n){return px(n)?t(...n):t(n)}function Cc(t){return oe(n=>gx(t,n))}var{isArray:vx}=Array,{getPrototypeOf:bx,prototype:_x,keys:yx}=Object;function Dc(t){if(t.length===1){let n=t[0];if(vx(n))return{args:n,keys:null};if(Sx(n)){let e=yx(n);return{args:e.map(i=>n[i]),keys:e}}}return{args:t,keys:null}}function Sx(t){return t&&typeof t=="object"&&bx(t)===_x}function Ec(t,n){return t.reduce((e,i,r)=>(e[i]=n[r],e),{})}function Ys(...t){let n=An(t),e=cc(t),{args:i,keys:r}=Dc(t);if(i.length===0)return Ve([],n);let o=new ne(Cx(i,n,r?s=>Ec(r,s):Lt));return e?o.pipe(Cc(e)):o}function Cx(t,n,e=Lt){return i=>{Cb(n,()=>{let{length:r}=t,o=new Array(r),s=r,a=r;for(let l=0;l<r;l++)Cb(n,()=>{let c=Ve(t[l],n),u=!1;c.subscribe(fe(i,f=>{o[l]=f,u||(u=!0,a--),a||i.next(e(o.slice()))},()=>{--s||i.complete()}))},i)},i)}}function Cb(t,n,e){t?Rt(e,t,n):n()}function Db(t,n,e,i,r,o,s,a){let l=[],c=0,u=0,f=!1,h=()=>{f&&!l.length&&!c&&n.complete()},p=C=>c<i?S(C):l.push(C),S=C=>{o&&n.next(C),c++;let $=!1;ke(e(C,u++)).subscribe(fe(n,F=>{r?.(F),o?p(F):n.next(F)},()=>{$=!0},void 0,()=>{if($)try{for(c--;l.length&&c<i;){let F=l.shift();s?Rt(n,s,()=>S(F)):S(F)}h()}catch(F){n.error(F)}}))};return t.subscribe(fe(n,p,()=>{f=!0,h()})),()=>{a?.()}}function Tt(t,n,e=1/0){return ae(n)?Tt((i,r)=>oe((o,s)=>n(i,o,r,s))(ke(t(i,r))),e):(typeof n=="number"&&(e=n),ue((i,r)=>Db(i,r,t,e)))}function wc(t=1/0){return Tt(Lt,t)}function Eb(){return wc(1)}function Ni(...t){return Eb()(Ve(t,An(t)))}function cn(t){return new ne(n=>{ke(t()).subscribe(n)})}function Zs(...t){let n=cc(t),{args:e,keys:i}=Dc(t),r=new ne(o=>{let{length:s}=e;if(!s){o.complete();return}let a=new Array(s),l=s,c=s;for(let u=0;u<s;u++){let f=!1;ke(e[u]).subscribe(fe(o,h=>{f||(f=!0,c--),a[u]=h},()=>l--,void 0,()=>{(!l||!f)&&(c||o.next(i?Ec(i,a):a),o.complete())}))}});return n?r.pipe(Cc(n)):r}function wb(t=0,n,e=lb){let i=-1;return n!=null&&(lc(n)?e=n:i=n),new ne(r=>{let o=Sb(t)?+t-e.now():t;o<0&&(o=0);let s=0;return e.schedule(function(){r.closed||(r.next(s++),0<=i?this.schedule(void 0,i):r.complete())},o)})}function Nt(...t){let n=An(t),e=cb(t,1/0),i=t;return i.length?i.length===1?ke(i[0]):wc(e)(Ve(i,n)):Ge}function ge(t,n){return ue((e,i)=>{let r=0;e.subscribe(fe(i,o=>t.call(n,o,r++)&&i.next(o)))})}function xb(t){return ue((n,e)=>{let i=!1,r=null,o=null,s=!1,a=()=>{if(o?.unsubscribe(),o=null,i){i=!1;let c=r;r=null,e.next(c)}s&&e.complete()},l=()=>{o=null,s&&e.complete()};n.subscribe(fe(e,c=>{i=!0,r=c,o||ke(t(c)).subscribe(o=fe(e,a,l))},()=>{s=!0,(!i||!o||o.closed)&&e.complete()}))})}function xc(t,n=Gs){return xb(()=>wb(t,n))}function wr(t){return ue((n,e)=>{let i=null,r=!1,o;i=n.subscribe(fe(e,void 0,void 0,s=>{o=ke(t(s,wr(t)(n))),i?(i.unsubscribe(),i=null,o.subscribe(e)):r=!0})),r&&(i.unsubscribe(),i=null,o.subscribe(e))})}function No(t,n){return ae(n)?Tt(t,n,1):Tt(t,1)}function Xs(t,n=Gs){return ue((e,i)=>{let r=null,o=null,s=null,a=()=>{if(r){r.unsubscribe(),r=null;let c=o;o=null,i.next(c)}};function l(){let c=s+t,u=n.now();if(u<c){r=this.schedule(void 0,c-u),i.add(r);return}a()}e.subscribe(fe(i,c=>{o=c,s=n.now(),r||(r=n.schedule(l,t),i.add(r))},()=>{a(),i.complete()},void 0,()=>{o=r=null}))})}function Ib(t){return ue((n,e)=>{let i=!1;n.subscribe(fe(e,r=>{i=!0,e.next(r)},()=>{i||e.next(t),e.complete()}))})}function Ae(t){return t<=0?()=>Ge:ue((n,e)=>{let i=0;n.subscribe(fe(e,r=>{++i<=t&&(e.next(r),t<=i&&e.complete())}))})}function Ic(t,n=Lt){return t=t??Dx,ue((e,i)=>{let r,o=!0;e.subscribe(fe(i,s=>{let a=n(s);(o||!t(r,a))&&(o=!1,r=a,i.next(s))}))})}function Dx(t,n){return t===n}function Mb(t=Ex){return ue((n,e)=>{let i=!1;n.subscribe(fe(e,r=>{i=!0,e.next(r)},()=>i?e.complete():e.error(t())))})}function Ex(){return new Er}function xr(t){return ue((n,e)=>{try{n.subscribe(e)}finally{e.add(t)}})}function Kn(t,n){let e=arguments.length>=2;return i=>i.pipe(t?ge((r,o)=>t(r,o,i)):Lt,Ae(1),e?Ib(n):Mb(()=>new Er))}function Mc(t){return t<=0?()=>Ge:ue((n,e)=>{let i=[];n.subscribe(fe(e,r=>{i.push(r),t<i.length&&i.shift()},()=>{for(let r of i)e.next(r);e.complete()},void 0,()=>{i=null}))})}function Tc(){return ue((t,n)=>{let e,i=!1;t.subscribe(fe(n,r=>{let o=e;e=r,i&&n.next([o,r]),i=!0}))})}function Ks(t={}){let{connector:n=()=>new E,resetOnError:e=!0,resetOnComplete:i=!0,resetOnRefCountZero:r=!0}=t;return o=>{let s,a,l,c=0,u=!1,f=!1,h=()=>{a?.unsubscribe(),a=void 0},p=()=>{h(),s=l=void 0,u=f=!1},S=()=>{let C=s;p(),C?.unsubscribe()};return ue((C,$)=>{c++,!f&&!u&&h();let F=l=l??n();$.add(()=>{c--,c===0&&!f&&!u&&(a=sh(S,r))}),F.subscribe($),!s&&c>0&&(s=new Xn({next:we=>F.next(we),error:we=>{f=!0,h(),a=sh(p,e,we),F.error(we)},complete:()=>{u=!0,h(),a=sh(p,i),F.complete()}}),ke(C).subscribe(s))})(o)}}function sh(t,n,...e){if(n===!0){t();return}if(n===!1)return;let i=new Xn({next:()=>{i.unsubscribe(),t()}});return ke(n(...e)).subscribe(i)}function Nc(t,n,e){let i,r=!1;return t&&typeof t=="object"?{bufferSize:i=1/0,windowTime:n=1/0,refCount:r=!1,scheduler:e}=t:i=t??1/0,Ks({connector:()=>new Ti(i,n,e),resetOnError:!0,resetOnComplete:!1,resetOnRefCountZero:r})}function Qs(t){return ge((n,e)=>t<=e)}function nt(...t){let n=An(t);return ue((e,i)=>{(n?Ni(t,e,n):Ni(t,e)).subscribe(i)})}function Xe(t,n){return ue((e,i)=>{let r=null,o=0,s=!1,a=()=>s&&!r&&i.complete();e.subscribe(fe(i,l=>{r?.unsubscribe();let c=0,u=o++;ke(t(l,u)).subscribe(r=fe(i,f=>i.next(n?n(l,f,u,c++):f),()=>{r=null,a()}))},()=>{s=!0,a()}))})}function ye(t){return ue((n,e)=>{ke(t).subscribe(fe(e,()=>e.complete(),Us)),!e.closed&&n.subscribe(e)})}function ah(t,n=!1){return ue((e,i)=>{let r=0;e.subscribe(fe(i,o=>{let s=t(o,r++);(s||n)&&i.next(o),!s&&i.complete()}))})}function dt(t,n,e){let i=ae(t)||n||e?{next:t,error:n,complete:e}:t;return i?ue((r,o)=>{var s;(s=i.subscribe)===null||s===void 0||s.call(i);let a=!0;r.subscribe(fe(o,l=>{var c;(c=i.next)===null||c===void 0||c.call(i,l),o.next(l)},()=>{var l;a=!1,(l=i.complete)===null||l===void 0||l.call(i),o.complete()},l=>{var c;a=!1,(c=i.error)===null||c===void 0||c.call(i,l),o.error(l)},()=>{var l,c;a&&((l=i.unsubscribe)===null||l===void 0||l.call(i)),(c=i.finalize)===null||c===void 0||c.call(i)}))}):Lt}var Lc="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",x=class extends Error{code;constructor(n,e){super(ti(n,e)),this.code=n}};function wx(t){return`NG0${Math.abs(t)}`}function ti(t,n){return`${wx(t)}${n?": "+n:""}`}function De(t){for(let n in t)if(t[n]===De)return n;throw Error("")}function Rb(t,n){for(let e in n)Object.hasOwn(n,e)&&!Object.hasOwn(t,e)&&(t[e]=n[e])}function ra(t){if(typeof t=="string")return t;if(Array.isArray(t))return`[${t.map(ra).join(", ")}]`;if(t==null)return""+t;let n=t.overriddenName||t.name;if(n)return`${n}`;let e=t.toString();if(e==null)return""+e;let i=e.indexOf(`
`);return i>=0?e.slice(0,i):e}function Vc(t,n){return t?n?`${t} ${n}`:t:n||""}var xx=De({__forward_ref__:De});function Pt(t){return t.__forward_ref__=Pt,t}function it(t){return yh(t)?t():t}function yh(t){return typeof t=="function"&&Object.hasOwn(t,xx)&&t.__forward_ref__===Pt}function G(t){return{token:t.token,providedIn:t.providedIn||null,factory:t.factory,value:void 0}}function q(t){return{providers:t.providers||[],imports:t.imports||[]}}function oa(t){return Ix(t,Bc)}function Sh(t){return oa(t)!==null}function Ix(t,n){return Object.hasOwn(t,n)&&t[n]||null}function Mx(t){let n=t?.[Bc]??null;return n||null}function ch(t){return t&&Object.hasOwn(t,Ac)?t[Ac]:null}var Bc=De({\u0275prov:De}),Ac=De({\u0275inj:De}),b=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(n,e){this._desc=n,this.\u0275prov=void 0,typeof e=="number"?this.__NG_ELEMENT_ID__=e:e!==void 0&&(this.\u0275prov=G({token:this,providedIn:e.providedIn||"root",factory:e.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function Ch(t){return t&&!!t.\u0275providers}var sa=De({\u0275cmp:De}),aa=De({\u0275dir:De}),Dh=De({\u0275pipe:De}),Eh=De({\u0275mod:De}),ea=De({\u0275fac:De}),Ar=De({__NG_ELEMENT_ID__:De}),Tb=De({__NG_ENV_ID__:De});function Ob(t){return Hc(t,"@NgModule"),t[Eh]||null}function ki(t){return Hc(t,"@Component"),t[sa]||null}function jc(t){return Hc(t,"@Directive"),t[aa]||null}function Pb(t){return Hc(t,"@Pipe"),t[Dh]||null}function Hc(t,n){if(t==null)throw new x(-919,!1)}function Uc(t){return typeof t=="string"?t:t==null?"":String(t)}var Fb=De({ngErrorCode:De}),Tx=De({ngErrorMessage:De}),Nx=De({ngTokenPath:De});function wh(t,n){return Lb("",-200,n)}function zc(t,n){throw new x(-201,!1)}function Lb(t,n,e){let i=new x(n,t);return i[Fb]=n,i[Tx]=t,e&&(i[Nx]=e),i}function kx(t){return t[Fb]}var dh;function Vb(){return dh}function Vt(t){let n=dh;return dh=t,n}function xh(t,n,e){let i=oa(t);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(e&8)return null;if(n!==void 0)return n;zc(t,"")}var Ro=globalThis;var Ax={},Ir=Ax,Rx="__NG_DI_FLAG__",uh=class{injector;constructor(n){this.injector=n}retrieve(n,e){let i=Mr(e)||0;try{return this.injector.get(n,i&8?null:Ir,i)}catch(r){if(wo(r))return r;throw r}}};function Ox(t,n=0){let e=Ql();if(e===void 0)throw new x(-203,!1);if(e===null)return xh(t,void 0,n);{let i=Px(n),r=e.retrieve(t,i);if(wo(r)){if(i.optional)return null;throw r}return r}}function A(t,n=0){return(Vb()||Ox)(it(t),n)}function d(t,n){return A(t,Mr(n))}function Mr(t){return typeof t>"u"||typeof t=="number"?t:0|(t.optional&&8)|(t.host&&1)|(t.self&&2)|(t.skipSelf&&4)}function Px(t){return{optional:!!(t&8),host:!!(t&1),self:!!(t&2),skipSelf:!!(t&4)}}function fh(t){let n=[];for(let e=0;e<t.length;e++){let i=it(t[e]);if(Array.isArray(i)){if(i.length===0)throw new x(900,!1);let r,o=0;for(let s=0;s<i.length;s++){let a=i[s],l=Fx(a);typeof l=="number"?l===-1?r=a.token:o|=l:r=a}n.push(A(r,o))}else n.push(A(i))}return n}function Fx(t){return t[Rx]}function Tr(t,n){let e=Object.hasOwn(t,ea);return e?t[ea]:null}function Bb(t,n,e){if(t.length!==n.length)return!1;for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(e&&(r=e(r),o=e(o)),o!==r)return!1}return!0}function jb(t){return t.flat(Number.POSITIVE_INFINITY)}function $c(t,n){t.forEach(e=>Array.isArray(e)?$c(e,n):n(e))}function Ih(t,n,e){n>=t.length?t.push(e):t.splice(n,0,e)}function la(t,n){return n>=t.length-1?t.pop():t.splice(n,1)[0]}function Hb(t,n){let e=[];for(let i=0;i<t;i++)e.push(n);return e}function Ub(t,n,e,i){let r=t.length;if(r==n)t.push(e,i);else if(r===1)t.push(i,t[0]),t[0]=e;else{for(r--,t.push(t[r-1],t[r]);r>n;){let o=r-2;t[r]=t[o],r--}t[n]=e,t[n+1]=i}}function Gc(t,n,e){let i=Oo(t,n);return i>=0?t[i|1]=e:(i=~i,Ub(t,i,n,e)),i}function Wc(t,n){let e=Oo(t,n);if(e>=0)return t[e|1]}function Oo(t,n){return Lx(t,n,1)}function Lx(t,n,e){let i=0,r=t.length>>e;for(;r!==i;){let o=i+(r-i>>1),s=t[o<<e];if(n===s)return o<<e;s>n?r=o:i=o+1}return~(r<<e)}var Ai={},St=[],Rr=new b(""),ca=new b("",-1),Mh=new b(""),Ao=class{get(n,e=Ir){if(e===Ir){let r=Lb("",-201);throw r.name="\u0275NotFound",r}return e}};function On(t){return{\u0275providers:t}}function zb(t){return On([{provide:Rr,multi:!0,useValue:t}])}function $b(...t){return{\u0275providers:Th(!0,t),\u0275fromNgModule:!0}}function Th(t,...n){let e=[],i=new Set,r,o=s=>{e.push(s)};return $c(n,s=>{let a=s;Rc(a,o,[],i)&&(r||=[],r.push(a))}),r!==void 0&&Gb(r,o),e}function Gb(t,n){for(let e=0;e<t.length;e++){let{ngModule:i,providers:r}=t[e];Nh(r,o=>{n(o,i)})}}function Rc(t,n,e,i){if(t=it(t),!t)return!1;let r=null,o=ch(t),s=!o&&ki(t);if(!o&&!s){let l=t.ngModule;if(o=ch(l),o)r=l;else return!1}else{if(s&&!s.standalone)return!1;r=t}let a=i.has(r);if(s){if(a)return!1;if(i.add(r),s.dependencies){let l=typeof s.dependencies=="function"?s.dependencies():s.dependencies;for(let c of l)Rc(c,n,e,i)}}else if(o){if(o.imports!=null&&!a){i.add(r);let c;$c(o.imports,u=>{Rc(u,n,e,i)&&(c||=[],c.push(u))}),c!==void 0&&Gb(c,n)}if(!a){let c=Tr(r)||(()=>new r);n({provide:r,useFactory:c,deps:St},r),n({provide:Mh,useValue:r,multi:!0},r),n({provide:Rr,useValue:()=>A(r),multi:!0},r)}let l=o.providers;if(l!=null&&!a){let c=t;Nh(l,u=>{n(u,c)})}}else return!1;return r!==t&&t.providers!==void 0}function Nh(t,n){for(let e of t)Ch(e)&&(e=e.\u0275providers),Array.isArray(e)?Nh(e,n):n(e)}var Vx=De({provide:String,useValue:De});function Wb(t){return t!==null&&typeof t=="object"&&Vx in t}function Bx(t){return!!(t&&t.useExisting)}function jx(t){return!!(t&&t.useFactory)}function Nr(t){return typeof t=="function"}function qb(t){return!!t.useClass}var da=new b(""),kc={},Nb={},lh;function Po(){return lh===void 0&&(lh=new Ao),lh}var Ie=class{},kr=class extends Ie{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(n,e,i,r){super(),this.parent=e,this.source=i,this.scopes=r,mh(n,s=>this.processProvider(s)),this.records.set(ca,ko(void 0,this)),r.has("environment")&&this.records.set(Ie,ko(void 0,this));let o=this.records.get(da);o!=null&&typeof o.value=="string"&&this.scopes.add(o.value),this.injectorDefTypes=new Set(this.get(Mh,St,{self:!0}))}retrieve(n,e){let i=Mr(e)||0;try{return this.get(n,Ir,i)}catch(r){if(wo(r))return r;throw r}}destroy(){Js(this),this._destroyed=!0;let n=K(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let e=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of e)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),K(n)}}onDestroy(n){return Js(this),this._onDestroyHooks.push(n),()=>this.removeOnDestroy(n)}runInContext(n){Js(this);let e=kn(this),i=Vt(void 0),r;try{return n()}finally{kn(e),Vt(i)}}get(n,e=Ir,i){if(Js(this),Object.hasOwn(n,Tb))return n[Tb](this);let r=Mr(i),o,s=kn(this),a=Vt(void 0);try{if(!(r&4)){let c=this.records.get(n);if(c===void 0){let u=Gx(n)&&oa(n);u&&this.injectableDefInScope(u)?c=ko(hh(n),kc):c=null,this.records.set(n,c)}if(c!=null)return this.hydrate(n,c,r)}let l=r&2?Po():this.parent;return e=r&8&&e===Ir?null:e,l.get(n,e)}catch(l){let c=kx(l);throw c===-200||c===-201?new x(c,null):l}finally{Vt(a),kn(s)}}resolveInjectorInitializers(){let n=K(null),e=kn(this),i=Vt(void 0),r;try{let o=this.get(Rr,St,{self:!0});for(let s of o)s()}finally{kn(e),Vt(i),K(n)}}toString(){return"R3Injector[...]"}processProvider(n){n=it(n);let e=Nr(n)?n:it(n&&n.provide),i=Ux(n);if(!Nr(n)&&n.multi===!0){let r=this.records.get(e);r||(r=ko(void 0,kc,!0),r.factory=()=>fh(r.multi),this.records.set(e,r)),e=n,r.multi.push(n)}this.records.set(e,i)}hydrate(n,e,i){let r=K(null);try{if(e.value===Nb)throw wh("");return e.value===kc&&(e.value=Nb,e.value=e.factory(void 0,i)),typeof e.value=="object"&&e.value&&$x(e.value)&&this._ngOnDestroyHooks.add(e.value),e.value}finally{K(r)}}injectableDefInScope(n){if(!n.providedIn)return!1;let e=it(n.providedIn);return typeof e=="string"?e==="any"||this.scopes.has(e):this.injectorDefTypes.has(e)}removeOnDestroy(n){let e=this._onDestroyHooks.indexOf(n);e!==-1&&this._onDestroyHooks.splice(e,1)}};function hh(t){let n=oa(t),e=n!==null?n.factory:Tr(t);if(e!==null)return e;if(t instanceof b)throw new x(-204,!1);if(t instanceof Function)return Hx(t);throw new x(-204,!1)}function Hx(t){if(t.length>0)throw new x(-204,!1);let e=Mx(t);return e!==null?()=>e.factory(t):()=>new t}function Ux(t){if(Wb(t))return ko(void 0,t.useValue);{let n=kh(t);return ko(n,kc)}}function kh(t,n,e){let i;if(Nr(t)){let r=it(t);return Tr(r)||hh(r)}else if(Wb(t))i=()=>it(t.useValue);else if(jx(t))i=()=>t.useFactory(...fh(t.deps||[]));else if(Bx(t))i=(r,o)=>A(it(t.useExisting),o!==void 0&&o&8?8:void 0);else{let r=it(t&&(t.useClass||t.provide));if(zx(t))i=()=>new r(...fh(t.deps));else return Tr(r)||hh(r)}return i}function Js(t){if(t.destroyed)throw new x(-205,!1)}function ko(t,n,e=!1){return{factory:t,value:n,multi:e?[]:void 0}}function zx(t){return!!t.deps}function $x(t){return t!==null&&typeof t=="object"&&typeof t.ngOnDestroy=="function"}function Gx(t){return typeof t=="function"||typeof t=="object"&&t.ngMetadataName==="InjectionToken"}function mh(t,n){for(let e of t)Array.isArray(e)?mh(e,n):e&&Ch(e)?mh(e.\u0275providers,n):n(e)}function ot(t,n){let e;t instanceof kr?(Js(t),e=t):e=new uh(t);let i,r=kn(e),o=Vt(void 0);try{return n()}finally{kn(r),Vt(o)}}function Yb(){return Vb()!==void 0||Ql()!=null}var dn=0,Z=1,J=2,rt=3,Gt=4,pt=5,Or=6,Fo=7,qe=8,Pn=9,un=10,Fe=11,Lo=12,Ah=13,Ri=14,kt=15,Oi=16,Pr=17,Fn=18,Ln=19,Rh=20,Qn=21,qc=22,Jn=23,Bt=24,Fr=25,Vn=26,Ye=27,Zb=1,Oh=6,Lr=7,ua=8,Vr=9,je=10;function ni(t){return Array.isArray(t)&&typeof t[Zb]=="object"}function Wt(t){return Array.isArray(t)&&t[Zb]===!0}function Ph(t){return(t.flags&4)!==0}function ii(t){return t.componentOffset>-1}function fa(t){return(t.flags&1)===1}function fn(t){return!!t.template}function Vo(t){return(t[J]&512)!==0}function Br(t){return(t[J]&256)===256}var st=(function(t){return t[t.NONE=0]="NONE",t[t.HTML=1]="HTML",t[t.STYLE=2]="STYLE",t[t.SCRIPT=3]="SCRIPT",t[t.URL=4]="URL",t[t.RESOURCE_URL=5]="RESOURCE_URL",t[t.ATTRIBUTE_NO_BINDING=6]="ATTRIBUTE_NO_BINDING",t})(st||{});var Fh="svg",Xb="math";function gt(t){for(;Array.isArray(t);)t=t[dn];return t}function Lh(t,n){return gt(n[t])}function hn(t,n){return gt(n[t.index])}function Yc(t,n){return t.data[n]}function Kb(t,n){return t[n]}function qt(t,n){let e=n[t];return ni(e)?e:e[dn]}function Qb(t){return(t[J]&4)===4}function Zc(t){return(t[J]&128)===128}function Jb(t){return Wt(t[rt])}function jt(t,n){return n==null?null:t[n]}function Vh(t){t[Pr]=0}function Bh(t){t[J]&1024||(t[J]|=1024,Zc(t)&&jr(t))}function e_(t,n){for(;t>0;)n=n[Ri],t--;return n}function ha(t){return!!(t[J]&9216||t[Bt]?.dirty)}function Xc(t){t[un].changeDetectionScheduler?.notify(8),t[J]&64&&(t[J]|=1024),ha(t)&&jr(t)}function jr(t){t[un].changeDetectionScheduler?.notify(0);let n=ei(t);for(;n!==null&&!(n[J]&8192||(n[J]|=8192,!Zc(n)));)n=ei(n)}function Kc(t,n){if(Br(t))throw new x(911,!1);t[Qn]===null&&(t[Qn]=[]),t[Qn].push(n)}function t_(t,n){if(t[Qn]===null)return;let e=t[Qn].indexOf(n);e!==-1&&t[Qn].splice(e,1)}function ei(t){let n=t[rt];return Wt(n)?n[rt]:n}function jh(t){return t[Fo]??=[]}function Hh(t){return t.cleanup??=[]}function n_(t,n,e,i){let r=jh(n);r.push(e),t.firstCreatePass&&Hh(t).push(i,r.length-1)}var me={lFrame:h_(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var ph=!1;function i_(){return me.lFrame.elementDepthCount}function r_(){me.lFrame.elementDepthCount++}function Uh(){me.lFrame.elementDepthCount--}function zh(){return me.bindingsEnabled}function $h(){return me.skipHydrationRootTNode!==null}function Gh(t){return me.skipHydrationRootTNode===t}function Wh(){me.skipHydrationRootTNode=null}function le(){return me.lFrame.lView}function He(){return me.lFrame.tView}function ee(t){return me.lFrame.contextLView=t,t[qe]}function te(t){return me.lFrame.contextLView=null,t}function vt(){let t=qh();for(;t!==null&&t.type===64;)t=t.parent;return t}function qh(){return me.lFrame.currentTNode}function o_(){let t=me.lFrame,n=t.currentTNode;return t.isParent?n:n.parent}function Bo(t,n){let e=me.lFrame;e.currentTNode=t,e.isParent=n}function Yh(){return me.lFrame.isParent}function Zh(){me.lFrame.isParent=!1}function s_(){return me.lFrame.contextLView}function Xh(){return ph}function ta(t){let n=ph;return ph=t,n}function a_(t){return me.lFrame.bindingIndex=t}function Hr(){return me.lFrame.bindingIndex++}function Kh(t){let n=me.lFrame,e=n.bindingIndex;return n.bindingIndex=n.bindingIndex+t,e}function l_(){return me.lFrame.inI18n}function c_(t,n){let e=me.lFrame;e.bindingIndex=e.bindingRootIndex=t,Qc(n)}function d_(){return me.lFrame.currentDirectiveIndex}function Qc(t){me.lFrame.currentDirectiveIndex=t}function u_(t){let n=me.lFrame.currentDirectiveIndex;return n===-1?null:t[n]}function Jc(){return me.lFrame.currentQueryIndex}function ma(t){me.lFrame.currentQueryIndex=t}function Wx(t){let n=t[Z];return n.type===2?n.declTNode:n.type===1?t[pt]:null}function Qh(t,n,e){if(e&4){let r=n,o=t;for(;r=r.parent,r===null&&!(e&1);)if(r=Wx(o),r===null||(o=o[Ri],r.type&10))break;if(r===null)return!1;n=r,t=o}let i=me.lFrame=f_();return i.currentTNode=n,i.lView=t,!0}function ed(t){let n=f_(),e=t[Z];me.lFrame=n,n.currentTNode=e.firstChild,n.lView=t,n.tView=e,n.contextLView=t,n.bindingIndex=e.bindingStartIndex,n.inI18n=!1}function f_(){let t=me.lFrame,n=t===null?null:t.child;return n===null?h_(t):n}function h_(t){let n={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:t,child:null,inI18n:!1};return t!==null&&(t.child=n),n}function m_(){let t=me.lFrame;return me.lFrame=t.parent,t.currentTNode=null,t.lView=null,t}var Jh=m_;function td(){let t=m_();t.isParent=!0,t.tView=null,t.selectedIndex=-1,t.contextLView=null,t.elementDepthCount=0,t.currentDirectiveIndex=-1,t.currentNamespace=null,t.bindingRootIndex=-1,t.bindingIndex=-1,t.currentQueryIndex=0}function p_(t){return(me.lFrame.contextLView=e_(t,me.lFrame.contextLView))[qe]}function ri(){return me.lFrame.selectedIndex}function Pi(t){me.lFrame.selectedIndex=t}function nd(){let t=me.lFrame;return Yc(t.tView,t.selectedIndex)}function Fi(){me.lFrame.currentNamespace=Fh}function id(){qx()}function qx(){me.lFrame.currentNamespace=null}function em(){return me.lFrame.currentNamespace}var g_=!0;function rd(){return g_}function od(t){g_=t}function gh(t,n=null,e=null,i){let r=tm(t,n,e,i);return r.resolveInjectorInitializers(),r}function tm(t,n=null,e=null,i,r=new Set){let o=[e||St,$b(t)],s;return new kr(o,n||Po(),s||null,r)}var R=class t{static THROW_IF_NOT_FOUND=Ir;static NULL=new Ao;static create(n,e){if(Array.isArray(n))return gh({name:""},e,n,"");{let i=n.name??"";return gh({name:i},n.parent,n.providers,i)}}static \u0275prov=G({token:t,providedIn:"any",factory:()=>A(ca)});static __NG_ELEMENT_ID__=-1},L=new b(""),We=class{static __NG_ELEMENT_ID__=Yx;static __NG_ENV_ID__=n=>n},Oc=class extends We{_lView;constructor(n){super(),this._lView=n}get destroyed(){return Br(this._lView)}onDestroy(n){let e=this._lView;return Kc(e,n),()=>t_(e,n)}};function Yx(){return new Oc(le())}var v_=!1,b_=new b(""),oi=(()=>{class t{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new tt(!1);debugTaskTracker=d(b_,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new ne(e=>{e.next(!1),e.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let e=this.taskId++;return this.pendingTasks.add(e),this.debugTaskTracker?.add(e),e}has(e){return this.pendingTasks.has(e)}remove(e){this.pendingTasks.delete(e),this.debugTaskTracker?.remove(e),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=G({token:t,providedIn:"root",factory:()=>new t})}return t})(),vh=class extends E{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(n=!1){super(),this.__isAsync=n,Yb()&&(this.destroyRef=d(We,{optional:!0})??void 0,this.pendingTasks=d(oi,{optional:!0})??void 0)}emit(n){let e=K(null);try{super.next(n)}finally{K(e)}}subscribe(n,e,i){let r=n,o=e||(()=>null),s=i;if(n&&typeof n=="object"){let l=n;r=l.next?.bind(l),o=l.error?.bind(l),s=l.complete?.bind(l)}this.__isAsync&&(o=this.wrapInTimeout(o),r&&(r=this.wrapInTimeout(r)),s&&(s=this.wrapInTimeout(s)));let a=super.subscribe({next:r,error:o,complete:s});return n instanceof pe&&n.add(a),a}wrapInTimeout(n){return e=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{n(e)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},W=vh;function Pc(...t){}function nm(t){let n,e;function i(){t=Pc;try{e!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(e),n!==void 0&&clearTimeout(n)}catch{}}return n=setTimeout(()=>{t(),i()}),typeof requestAnimationFrame=="function"&&(e=requestAnimationFrame(()=>{t(),i()})),()=>i()}function __(t){return queueMicrotask(()=>t()),()=>{t=Pc}}var im="isAngularZone",na=im+"_ID",Zx=0,N=class t{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new W(!1);onMicrotaskEmpty=new W(!1);onStable=new W(!1);onError=new W(!1);constructor(n){let{enableLongStackTrace:e=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:o=v_}=n;if(typeof Zone>"u")throw new x(908,!1);Zone.assertZonePatched();let s=this;s._nesting=0,s._outer=s._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(s._inner=s._inner.fork(new Zone.TaskTrackingZoneSpec)),e&&Zone.longStackTraceZoneSpec&&(s._inner=s._inner.fork(Zone.longStackTraceZoneSpec)),s.shouldCoalesceEventChangeDetection=!r&&i,s.shouldCoalesceRunChangeDetection=r,s.callbackScheduled=!1,s.scheduleInRootZone=o,Qx(s)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(im)===!0}static assertInAngularZone(){if(!t.isInAngularZone())throw new x(909,!1)}static assertNotInAngularZone(){if(t.isInAngularZone())throw new x(909,!1)}run(n,e,i){return this._inner.run(n,e,i)}runTask(n,e,i,r){let o=this._inner,s=o.scheduleEventTask("NgZoneEvent: "+r,n,Xx,Pc,Pc);try{return o.runTask(s,e,i)}finally{o.cancelTask(s)}}runGuarded(n,e,i){return this._inner.runGuarded(n,e,i)}runOutsideAngular(n){return this._outer.run(n)}},Xx={};function rm(t){if(t._nesting==0&&!t.hasPendingMicrotasks&&!t.isStable)try{t._nesting++,t.onMicrotaskEmpty.emit(null)}finally{if(t._nesting--,!t.hasPendingMicrotasks)try{t.runOutsideAngular(()=>t.onStable.emit(null))}finally{t.isStable=!0}}}function Kx(t){if(t.isCheckStableRunning||t.callbackScheduled)return;t.callbackScheduled=!0;function n(){nm(()=>{t.callbackScheduled=!1,bh(t),t.isCheckStableRunning=!0,rm(t),t.isCheckStableRunning=!1})}t.scheduleInRootZone?Zone.root.run(()=>{n()}):t._outer.run(()=>{n()}),bh(t)}function Qx(t){let n=()=>{Kx(t)},e=Zx++;t._inner=t._inner.fork({name:"angular",properties:{[im]:!0,[na]:e,[na+e]:!0},onInvokeTask:(i,r,o,s,a,l)=>{if(Jx(l))return i.invokeTask(o,s,a,l);try{return kb(t),i.invokeTask(o,s,a,l)}finally{(t.shouldCoalesceEventChangeDetection&&s.type==="eventTask"||t.shouldCoalesceRunChangeDetection)&&n(),Ab(t)}},onInvoke:(i,r,o,s,a,l,c)=>{try{return kb(t),i.invoke(o,s,a,l,c)}finally{t.shouldCoalesceRunChangeDetection&&!t.callbackScheduled&&!eI(l)&&n(),Ab(t)}},onHasTask:(i,r,o,s)=>{i.hasTask(o,s),r===o&&(s.change=="microTask"?(t._hasPendingMicrotasks=s.microTask,bh(t),rm(t)):s.change=="macroTask"&&(t.hasPendingMacrotasks=s.macroTask))},onHandleError:(i,r,o,s)=>(i.handleError(o,s),t.runOutsideAngular(()=>t.onError.emit(s)),!1)})}function bh(t){t._hasPendingMicrotasks||(t.shouldCoalesceEventChangeDetection||t.shouldCoalesceRunChangeDetection)&&t.callbackScheduled===!0?t.hasPendingMicrotasks=!0:t.hasPendingMicrotasks=!1}function kb(t){t._nesting++,t.isStable&&(t.isStable=!1,t.onUnstable.emit(null))}function Ab(t){t._nesting--,rm(t)}var ia=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new W;onMicrotaskEmpty=new W;onStable=new W;onError=new W;run(n,e,i){return n.apply(e,i)}runGuarded(n,e,i){return n.apply(e,i)}runOutsideAngular(n){return n()}runTask(n,e,i,r){return n.apply(e,i)}};function Jx(t){return y_(t,"__ignore_ng_zone__")}function eI(t){return y_(t,"__scheduler_tick__")}function y_(t,n){return!Array.isArray(t)||t.length!==1?!1:t[0]?.data?.[n]===!0}var Ot=class{_console=console;handleError(n){this._console.error("ERROR",n)}},Yt=new b("",{factory:()=>{let t=d(N),n=d(Ie),e;return i=>{t.runOutsideAngular(()=>{n.destroyed&&!e?setTimeout(()=>{throw i}):(e??=n.get(Ot),e.handleError(i))})}}}),S_={provide:Rr,useValue:()=>{let t=d(Ot,{optional:!0})},multi:!0},tI=new b("",{factory:()=>{let t=d(L).defaultView;if(!t)return;let n=d(Yt),e=o=>{n(o.reason),o.preventDefault()},i=o=>{o.error?n(o.error):n(new Error(o.message,{cause:o})),o.preventDefault()},r=()=>{t.addEventListener("unhandledrejection",e),t.addEventListener("error",i)};typeof Zone<"u"?Zone.root.run(r):r(),d(We).onDestroy(()=>{t.removeEventListener("error",i),t.removeEventListener("unhandledrejection",e)})}});function om(){return On([zb(()=>{d(tI)})])}function j(t,n){let[e,i,r]=Wf(t,n?.equal),o=e,s=o[et];return o.set=i,o.update=r,o.asReadonly=C_.bind(o),o}function C_(){let t=this[et];if(t.readonlyFn===void 0){let n=()=>this();n[et]=t,t.readonlyFn=n}return t.readonlyFn}var si=new b("",{factory:()=>nI}),nI="ng";var sd=new b(""),Ur=new b("",{providedIn:"platform",factory:()=>"unknown"}),pa=new b(""),Li=new b("",{factory:()=>d(L).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var jo=(()=>{class t{view;node;constructor(e,i){this.view=e,this.node=i}static __NG_ELEMENT_ID__=iI}return t})();function iI(){return new jo(le(),vt())}var Rn=class{},ga=new b("",{factory:()=>!0});var sm=new b(""),ad=(()=>{class t{static \u0275prov=G({token:t,providedIn:"root",factory:()=>new _h})}return t})(),_h=class{dirtyEffectCount=0;queues=new Map;add(n){this.enqueue(n),this.schedule(n)}schedule(n){n.dirty&&this.dirtyEffectCount++}remove(n){let e=n.zone,i=this.queues.get(e);i.has(n)&&(i.delete(n),n.dirty&&this.dirtyEffectCount--)}enqueue(n){let e=n.zone;this.queues.has(e)||this.queues.set(e,new Set);let i=this.queues.get(e);i.has(n)||i.add(n)}flush(){for(;this.dirtyEffectCount>0;){let n=!1;for(let[e,i]of this.queues)e===null?n||=this.flushQueue(i):n||=e.run(()=>this.flushQueue(i));n||(this.dirtyEffectCount=0)}}flushQueue(n){let e=!1;for(let i of n)i.dirty&&(this.dirtyEffectCount--,e=!0,i.run());return e}},Fc=class{[et];constructor(n){this[et]=n}destroy(){this[et].destroy()}};function ut(t,n){let e=n?.injector??d(R),i=n?.manualCleanup!==!0?e.get(We):null,r,o=e.get(jo,null,{optional:!0}),s=e.get(Rn);return o!==null?(r=E_(o.view,s,t),i instanceof Oc&&i._lView===o.view&&(i=null)):r=sI(t,e.get(ad),s),r.injector=e,i!==null&&(r.onDestroyFns=[i.onDestroy(()=>r.destroy())]),new Fc(r)}var D_=k(v({},Yf),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let t=ta(!1);try{Zf(this)}finally{ta(t)}},cleanup(){if(!this.cleanupFns?.length)return;let t=K(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],K(t)}}}),rI=k(v({},D_),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(Mi(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.scheduler.remove(this)}}),oI=k(v({},D_),{consumerMarkedDirty(){this.view[J]|=8192,jr(this.view),this.notifier.notify(13)},destroy(){if(Mi(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.view[Jn]?.delete(this)}});function E_(t,n,e){let i=Object.create(oI);return i.view=t,i.zone=typeof Zone<"u"?Zone.current:null,i.notifier=n,i.fn=w_(i,e),t[Jn]??=new Set,t[Jn].add(i),i.consumerMarkedDirty(i),i}function sI(t,n,e){let i=Object.create(rI);return i.fn=w_(i,t),i.scheduler=n,i.notifier=e,i.zone=typeof Zone<"u"?Zone.current:null,i.scheduler.add(i),i.notifier.notify(12),i}function w_(t,n){return()=>{n(e=>(t.cleanupFns??=[]).push(e))}}function Zt(t){return typeof t=="function"&&t[et]!==void 0}var ld=(()=>{class t{internalPendingTasks=d(oi);scheduler=d(Rn);errorHandler=d(Yt);add(){let e=this.internalPendingTasks.add();return()=>{this.internalPendingTasks.has(e)&&(this.scheduler.notify(11),this.internalPendingTasks.remove(e))}}run(e){let i=this.add();try{e().catch(this.errorHandler).finally(i)}catch(r){this.errorHandler(r),i()}}static \u0275prov=G({token:t,providedIn:"root",factory:()=>new t})}return t})();var ep=Symbol("InputSignalNode#UNSET"),dy=k(v({},Eo),{transformFn:void 0,applyValueToInputSignal(t,n){_r(t,n)}});function xa(t){return{toString:t}.toString()}var _e=(function(t){return t[t.TemplateCreateStart=0]="TemplateCreateStart",t[t.TemplateCreateEnd=1]="TemplateCreateEnd",t[t.TemplateUpdateStart=2]="TemplateUpdateStart",t[t.TemplateUpdateEnd=3]="TemplateUpdateEnd",t[t.LifecycleHookStart=4]="LifecycleHookStart",t[t.LifecycleHookEnd=5]="LifecycleHookEnd",t[t.OutputStart=6]="OutputStart",t[t.OutputEnd=7]="OutputEnd",t[t.BootstrapApplicationStart=8]="BootstrapApplicationStart",t[t.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",t[t.BootstrapComponentStart=10]="BootstrapComponentStart",t[t.BootstrapComponentEnd=11]="BootstrapComponentEnd",t[t.ChangeDetectionStart=12]="ChangeDetectionStart",t[t.ChangeDetectionEnd=13]="ChangeDetectionEnd",t[t.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",t[t.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",t[t.AfterRenderHooksStart=16]="AfterRenderHooksStart",t[t.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",t[t.ComponentStart=18]="ComponentStart",t[t.ComponentEnd=19]="ComponentEnd",t[t.DeferBlockStateStart=20]="DeferBlockStateStart",t[t.DeferBlockStateEnd=21]="DeferBlockStateEnd",t[t.DynamicComponentStart=22]="DynamicComponentStart",t[t.DynamicComponentEnd=23]="DynamicComponentEnd",t[t.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",t[t.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",t})(_e||{}),vd=class{previousValue;currentValue;firstChange;constructor(n,e,i){this.previousValue=n,this.currentValue=e,this.firstChange=i}isFirstChange(){return this.firstChange}};function uy(t,n,e,i){n!==null?n.applyValueToInputSignal(n,i):t[e]=i}var fy=null,at=(()=>{fy=x_;let t=()=>x_;return t.ngInherit=!0,t})();function bI(){return fy}function x_(t){return t.type.prototype.ngOnChanges&&(t.setInput=yI),_I}function _I(){let t=hy(this),n=t?.current;if(n){let e=t.previous;if(e===Ai)t.previous=n;else for(let i in n)e[i]=n[i];t.current=null,this.ngOnChanges(n)}}function yI(t,n,e,i,r){let o=this.declaredInputs[i],s=hy(t)||SI(t,{previous:Ai,current:null}),a=s.current||(s.current={}),l=s.previous,c=l[o];a[o]=new vd(c&&c.currentValue,e,l===Ai),uy(t,n,r,e)}var vm="__ngSimpleChanges__";function hy(t){return Object.hasOwn(t,vm)&&t[vm]||null}function SI(t,n){return t[vm]=n}var I_=[];var Ee=function(t,n=null,e){for(let i=0;i<I_.length;i++){let r=I_[i];r(t,n,e)}};function CI(t,n,e){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:o}=n.type.prototype;if(i){let s=bI()(n);(e.preOrderHooks??=[]).push(t,s),(e.preOrderCheckHooks??=[]).push(t,s)}r&&(e.preOrderHooks??=[]).push(0-t,r),o&&((e.preOrderHooks??=[]).push(t,o),(e.preOrderCheckHooks??=[]).push(t,o))}function my(t,n){for(let e=n.directiveStart,i=n.directiveEnd;e<i;e++){let o=t.data[e].type.prototype,{ngAfterContentInit:s,ngAfterContentChecked:a,ngAfterViewInit:l,ngAfterViewChecked:c,ngOnDestroy:u}=o;s&&(t.contentHooks??=[]).push(-e,s),a&&((t.contentHooks??=[]).push(e,a),(t.contentCheckHooks??=[]).push(e,a)),l&&(t.viewHooks??=[]).push(-e,l),c&&((t.viewHooks??=[]).push(e,c),(t.viewCheckHooks??=[]).push(e,c)),u!=null&&(t.destroyHooks??=[]).push(e,u)}}function fd(t,n,e){py(t,n,3,e)}function hd(t,n,e,i){(t[J]&3)===e&&py(t,n,e,i)}function am(t,n){let e=t[J];(e&3)===n&&(e&=16383,e+=1,t[J]=e)}function py(t,n,e,i){let r=i!==void 0?t[Pr]&65535:0,o=i??-1,s=n.length-1,a=0;for(let l=r;l<s;l++)if(typeof n[l+1]=="number"){if(a=n[l],i!=null&&a>=i)break}else n[l]<0&&(t[Pr]+=65536),(a<o||o==-1)&&(DI(t,e,n,l),t[Pr]=(t[Pr]&4294901760)+l+2),l++}function M_(t,n){Ee(_e.LifecycleHookStart,t,n);let e=K(null);try{n.call(t)}finally{K(e),Ee(_e.LifecycleHookEnd,t,n)}}function DI(t,n,e,i){let r=e[i]<0,o=e[i+1],s=r?-e[i]:e[i],a=t[s];r?t[J]>>14<t[Pr]>>16&&(t[J]&3)===n&&(t[J]+=16384,M_(a,o)):M_(a,o)}var Uo=-1,$r=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(n,e,i,r){this.factory=n,this.name=r,this.canSeeViewProviders=e,this.injectImpl=i}};function EI(t){return(t.flags&8)!==0}function wI(t){return(t.flags&16)!==0}function xI(t,n,e){let i=0;for(;i<e.length;){let r=e[i];if(typeof r=="number"){if(r!==0)break;i++;let o=e[i++],s=e[i++],a=e[i++];t.setAttribute(n,s,a,o)}else{let o=r,s=e[++i];II(o)?t.setProperty(n,o,s):t.setAttribute(n,o,s),i++}}return i}function gy(t){return t===3||t===4||t===6}function II(t){return t.charCodeAt(0)===64}function zo(t,n){if(!(n===null||n.length===0))if(t===null||t.length===0)t=n.slice();else{let e=-1;for(let i=0;i<n.length;i++){let r=n[i];typeof r=="number"?e=r:e===0||(e===-1||e===2?T_(t,e,r,null,n[++i]):T_(t,e,r,null,null))}}return t}function T_(t,n,e,i,r){let o=0,s=t.length;if(n===-1)s=-1;else for(;o<t.length;){let a=t[o++];if(typeof a=="number"){if(a===n){s=-1;break}else if(a>n){s=o-1;break}}}for(;o<t.length;){let a=t[o];if(typeof a=="number")break;if(a===e){r!==null&&(t[o+1]=r);return}o++,r!==null&&o++}s!==-1&&(t.splice(s,0,n),o=s+1),t.splice(o++,0,e),r!==null&&t.splice(o++,0,r)}function vy(t){return t!==Uo}function bd(t){return t&32767}function MI(t){return t>>16}function _d(t,n){let e=MI(t),i=n;for(;e>0;)i=i[Ri],e--;return i}var bm=!0;function N_(t){let n=bm;return bm=t,n}var TI=256,by=TI-1,_y=5,NI=0,Bn={};function kI(t,n,e){let i;typeof e=="string"?i=e.charCodeAt(0)||0:Object.hasOwn(e,Ar)&&(i=e[Ar]),i==null&&(i=e[Ar]=NI++);let r=i&by,o=1<<r;n.data[t+(r>>_y)]|=o}function yd(t,n){let e=yy(t,n);if(e!==-1)return e;let i=n[Z];i.firstCreatePass&&(t.injectorIndex=n.length,lm(i.data,t),lm(n,null),lm(i.blueprint,null));let r=tp(t,n),o=t.injectorIndex;if(vy(r)){let s=bd(r),a=_d(r,n),l=a[Z].data;for(let c=0;c<8;c++)n[o+c]=a[s+c]|l[s+c]}return n[o+8]=r,o}function lm(t,n){t.push(0,0,0,0,0,0,0,0,n)}function yy(t,n){return t.injectorIndex===-1||t.parent&&t.parent.injectorIndex===t.injectorIndex||n[t.injectorIndex+8]===null?-1:t.injectorIndex}function tp(t,n){if(t.parent&&t.parent.injectorIndex!==-1)return t.parent.injectorIndex;let e=0,i=null,r=n;for(;r!==null;){if(i=wy(r),i===null)return Uo;if(e++,r=r[Ri],i.injectorIndex!==-1)return i.injectorIndex|e<<16}return Uo}function _m(t,n,e){kI(t,n,e)}function AI(t,n){if(n==="class")return t.classes;if(n==="style")return t.styles;let e=t.attrs;if(e){let i=e.length,r=0;for(;r<i;){let o=e[r];if(gy(o))break;if(o===0)r=r+2;else if(typeof o=="number")for(r++;r<i&&typeof e[r]=="string";)r++;else{if(o===n)return e[r+1];r=r+2}}}return null}function Sy(t,n,e){if(e&8||t!==void 0)return t;zc(n,"NodeInjector")}function Cy(t,n,e,i){if(e&8&&i===void 0&&(i=null),(e&3)===0){let r=t[Pn],o=Vt(void 0);try{return r?r.get(n,i,e&8):xh(n,i,e&8)}finally{Vt(o)}}return Sy(i,n,e)}function Dy(t,n,e,i=0,r){if(t!==null){if(n[J]&2048&&!(i&2)){let s=FI(t,n,e,i,Bn);if(s!==Bn)return s}let o=Ey(t,n,e,i,Bn);if(o!==Bn)return o}return Cy(n,e,i,r)}function Ey(t,n,e,i,r){let o=OI(e);if(typeof o=="function"){if(!Qh(n,t,i))return i&1?Sy(r,e,i):Cy(n,e,i,r);try{let s;if(s=o(i),s==null&&!(i&8))zc(e);else return s}finally{Jh()}}else if(typeof o=="number"){let s=null,a=yy(t,n),l=Uo,c=i&1?n[kt][pt]:null;for((a===-1||i&4)&&(l=a===-1?tp(t,n):n[a+8],l===Uo||!A_(i,!1)?a=-1:(s=n[Z],a=bd(l),n=_d(l,n)));a!==-1;){let u=n[Z];if(k_(o,a,u.data)){let f=RI(a,n,e,s,i,c);if(f!==Bn)return f}l=n[a+8],l!==Uo&&A_(i,n[Z].data[a+8]===c)&&k_(o,a,n)?(s=u,a=bd(l),n=_d(l,n)):a=-1}}return r}function RI(t,n,e,i,r,o){let s=n[Z],a=s.data[t+8],l=i==null?ii(a)&&bm:i!=s&&(a.type&3)!==0,c=r&1&&o===a,u=md(a,s,e,l,c);return u!==null?ya(n,s,u,a,r):Bn}function md(t,n,e,i,r){let o=t.providerIndexes,s=n.data,a=o&1048575,l=t.directiveStart,c=t.directiveEnd,u=o>>20,f=i?a:a+u,h=r?a+u:c;for(let p=f;p<h;p++){let S=s[p];if(p<l&&e===S||p>=l&&S.type===e)return p}if(r){let p=s[l];if(p&&fn(p)&&p.type===e)return l}return null}function ya(t,n,e,i,r){let o=t[e],s=n.data;if(o instanceof $r){let a=o;if(a.resolving)throw wh("");let l=N_(a.canSeeViewProviders);a.resolving=!0;let c=s[e].type||s[e],u,f=a.injectImpl?Vt(a.injectImpl):null,h=Qh(t,i,0);try{o=t[e]=a.factory(void 0,r,s,t,i),n.firstCreatePass&&e>=i.directiveStart&&CI(e,s[e],n)}finally{f!==null&&Vt(f),N_(l),a.resolving=!1,Jh()}}return o}function OI(t){if(typeof t=="string")return t.charCodeAt(0)||0;let n=Object.hasOwn(t,Ar)?t[Ar]:void 0;return typeof n=="number"?n>=0?n&by:PI:n}function k_(t,n,e){let i=1<<t;return!!(e[n+(t>>_y)]&i)}function A_(t,n){return!(t&2)&&!(t&1&&n)}var Vi=class{_tNode;_lView;constructor(n,e){this._tNode=n,this._lView=e}get(n,e,i){return Dy(this._tNode,this._lView,n,Mr(i),e)}};function PI(){return new Vi(vt(),le())}function Ht(t){return xa(()=>{let n=t.prototype.constructor,e=n[ea]||ym(n),i=Object.prototype,r=Object.getPrototypeOf(t.prototype).constructor;for(;r&&r!==i;){let o=r[ea]||ym(r);if(o&&o!==e)return o;r=Object.getPrototypeOf(r)}return o=>new o})}function ym(t){return yh(t)?()=>{let n=ym(it(t));return n&&n()}:Tr(t)}function FI(t,n,e,i,r){let o=t,s=n;for(;o!==null&&s!==null&&s[J]&2048&&!Vo(s);){let a=Ey(o,s,e,i|2,Bn);if(a!==Bn)return a;i&=-5;let l=o.parent;if(!l){let c=s[Rh];if(c){let u=c.get(e,Bn,i);if(u!==Bn)return u}l=wy(s),s=s[Ri]}o=l}return r}function wy(t){let n=t[Z],e=n.type;return e===2?n.declTNode:e===1?t[pt]:null}function np(t){return AI(vt(),t)}function I(t){return{token:t.token,providedIn:t.autoProvided===!1?null:"root",factory:t.factory,value:void 0}}function LI(){return Zo(vt(),le())}function Zo(t,n){return new O(hn(t,n))}var O=(()=>{class t{nativeElement;constructor(e){this.nativeElement=e}static __NG_ELEMENT_ID__=LI}return t})();function xy(t){return t instanceof O?t.nativeElement:t}function VI(){return this._results[Symbol.iterator]()}var ai=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new E}constructor(n=!1){this._emitDistinctChangesOnly=n}get(n){return this._results[n]}map(n){return this._results.map(n)}filter(n){return this._results.filter(n)}find(n){return this._results.find(n)}reduce(n,e){return this._results.reduce(n,e)}forEach(n){this._results.forEach(n)}some(n){return this._results.some(n)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(n,e){this.dirty=!1;let i=jb(n);(this._changesDetected=!Bb(this._results,i,e))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(n){this._onDirty=n}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=VI};function Iy(t){return(t.flags&128)===128}var ip=(function(t){return t[t.OnPush=0]="OnPush",t[t.Eager=1]="Eager",t[t.Default=1]="Default",t})(ip||{}),My=new Map,BI=0;function jI(){return BI++}function HI(t){My.set(t[Ln],t)}function Sm(t){My.delete(t[Ln])}var R_="__ngContext__";function $o(t,n){ni(n)?(t[R_]=n[Ln],HI(n)):t[R_]=n}function Ty(t){return ky(t[Lo])}function Ny(t){return ky(t[Gt])}function ky(t){for(;t!==null&&!Wt(t);)t=t[Gt];return t}var Cm;function rp(t){Cm=t}function Ay(){if(Cm!==void 0)return Cm;if(typeof document<"u")return document;throw new x(210,!1)}var Ry="r";var Oy="di";var Py=!1,Fy=new b("",{factory:()=>Py});var O_=new WeakMap;function UI(t,n){if(t==null||typeof t!="object")return;let e=O_.get(t);e||(e=new WeakSet,O_.set(t,e)),e.add(n)}var zI=(t,n,e,i)=>{};function $I(t,n,e,i){zI(t,n,e,i)}function kd(t){return(t.flags&32)===32}var GI=()=>null;function Ly(t,n,e=!1){return GI(t,n,e)}function Vy(t,n){let e=t.contentQueries;if(e!==null){let i=K(null);try{for(let r=0;r<e.length;r+=2){let o=e[r],s=e[r+1];if(s!==-1){let a=t.data[s];ma(o),a.contentQueries(2,n[s],s)}}}finally{K(i)}}}function Dm(t,n,e){ma(0);let i=K(null);try{n(t,e)}finally{K(i)}}function By(t,n,e){if(Ph(n)){let i=K(null);try{let r=n.directiveStart,o=n.directiveEnd;for(let s=r;s<o;s++){let a=t.data[s];if(a.contentQueries){let l=e[s];a.contentQueries(1,l,s)}}}finally{K(i)}}}var gn=(function(t){return t[t.Emulated=0]="Emulated",t[t.None=2]="None",t[t.ShadowDom=3]="ShadowDom",t[t.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",t})(gn||{});var cd;function WI(){if(cd===void 0&&(cd=null,Ro.trustedTypes))try{cd=Ro.trustedTypes.createPolicy("angular",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch{}return cd}function Ad(t){return WI()?.createHTML(t)||t}var li=class{changingThisBreaksApplicationSecurity;constructor(n){this.changingThisBreaksApplicationSecurity=n}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${Lc})`}},Em=class extends li{getTypeName(){return"HTML"}},wm=class extends li{getTypeName(){return"Style"}},xm=class extends li{getTypeName(){return"Script"}},Im=class extends li{getTypeName(){return"URL"}},Mm=class extends li{getTypeName(){return"ResourceURL"}};function vn(t){return t instanceof li?t.changingThisBreaksApplicationSecurity:t}function Hi(t,n){let e=jy(t);if(e!=null&&e!==n){if(e==="ResourceURL"&&n==="URL")return!0;throw new Error(`Required a safe ${n}, got a ${e} (see ${Lc})`)}return e===n}function jy(t){return t instanceof li&&t.getTypeName()||null}function op(t){return new Em(t)}function sp(t){return new wm(t)}function ap(t){return new xm(t)}function lp(t){return new Im(t)}function cp(t){return new Mm(t)}function qI(t){let n=new Nm(t);return YI()?new Tm(n):n}var Tm=class{inertDocumentHelper;constructor(n){this.inertDocumentHelper=n}getInertBodyElement(n){n="<body><remove></remove>"+n;try{let e=new window.DOMParser().parseFromString(Ad(n),"text/html").body;return e===null?this.inertDocumentHelper.getInertBodyElement(n):(e.firstChild?.remove(),e)}catch{return null}}},Nm=class{defaultDoc;inertDocument;constructor(n){this.defaultDoc=n,this.inertDocument=this.defaultDoc.implementation.createHTMLDocument("sanitization-inert")}getInertBodyElement(n){let e=this.inertDocument.createElement("template");return e.innerHTML=Ad(n),e}};function YI(){try{return!!new window.DOMParser().parseFromString(Ad(""),"text/html")}catch{return!1}}var ZI=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function Ia(t){return t=String(t),t.match(ZI)?t:"unsafe:"+t}function ci(t){let n=Object.create(null);for(let e of t.split(","))n[e]=!0;return n}function Ma(...t){let n=Object.create(null);for(let e of t)for(let i in e)Object.hasOwn(e,i)&&(n[i]=!0);return n}var Hy=ci("area,br,col,hr,img,wbr"),Uy=ci("colgroup,dd,dt,li,p,tbody,td,tfoot,th,thead,tr"),zy=ci("rp,rt"),XI=Ma(zy,Uy),KI=Ma(Uy,ci("address,article,aside,blockquote,caption,center,del,details,dialog,dir,div,dl,figure,figcaption,footer,h1,h2,h3,h4,h5,h6,header,hgroup,hr,ins,main,map,menu,nav,ol,pre,section,summary,table,ul")),QI=Ma(zy,ci("a,abbr,acronym,audio,b,bdi,bdo,big,br,cite,code,del,dfn,em,font,i,img,ins,kbd,label,map,mark,picture,q,ruby,rp,rt,s,samp,small,source,span,strike,strong,sub,sup,time,track,tt,u,var,video")),P_=Ma(Hy,KI,QI,XI),$y=ci("background,cite,href,itemtype,longdesc,poster,src,xlink:href"),JI=ci("abbr,accesskey,align,alt,autoplay,axis,bgcolor,border,cellpadding,cellspacing,class,clear,color,cols,colspan,compact,controls,coords,datetime,default,dir,download,face,headers,height,hidden,hreflang,hspace,ismap,itemscope,itemprop,kind,label,lang,language,loop,media,muted,nohref,nowrap,open,preload,rel,rev,role,rows,rowspan,rules,scope,scrolling,shape,size,sizes,span,srclang,srcset,start,summary,tabindex,target,title,translate,type,usemap,valign,value,vspace,width"),eM=ci("aria-activedescendant,aria-atomic,aria-autocomplete,aria-busy,aria-checked,aria-colcount,aria-colindex,aria-colspan,aria-controls,aria-current,aria-describedby,aria-details,aria-disabled,aria-dropeffect,aria-errormessage,aria-expanded,aria-flowto,aria-grabbed,aria-haspopup,aria-hidden,aria-invalid,aria-keyshortcuts,aria-label,aria-labelledby,aria-level,aria-live,aria-modal,aria-multiline,aria-multiselectable,aria-orientation,aria-owns,aria-placeholder,aria-posinset,aria-pressed,aria-readonly,aria-relevant,aria-required,aria-roledescription,aria-rowcount,aria-rowindex,aria-rowspan,aria-selected,aria-setsize,aria-sort,aria-valuemax,aria-valuemin,aria-valuenow,aria-valuetext"),tM=Ma($y,JI,eM),nM=ci("script,style,template"),km=class{sanitizedSomething=!1;buf=[];sanitizeChildren(n){let e=n.firstChild,i=!0,r=[];for(;e;){if(e.nodeType===Node.ELEMENT_NODE?i=this.startElement(e):e.nodeType===Node.TEXT_NODE?this.chars(e.nodeValue):this.sanitizedSomething=!0,i&&e.firstChild){r.push(e),e=oM(e);continue}for(;e;){e.nodeType===Node.ELEMENT_NODE&&this.endElement(e);let o=rM(e);if(o){e=o;break}e=r.pop()}}return this.buf.join("")}startElement(n){let e=F_(n).toLowerCase();if(!Object.hasOwn(P_,e))return this.sanitizedSomething=!0,!Object.hasOwn(nM,e);this.buf.push("<"),this.buf.push(e);let i=n.attributes;for(let r=0;r<i.length;r++){let o=i.item(r),s=o.name,a=s.toLowerCase();if(!Object.hasOwn(tM,a)){this.sanitizedSomething=!0;continue}let l=o.value;$y[a]&&(l=Ia(l)),this.buf.push(" ",s,'="',L_(l),'"')}return this.buf.push(">"),!0}endElement(n){let e=F_(n).toLowerCase();Object.hasOwn(P_,e)&&!Object.hasOwn(Hy,e)&&(this.buf.push("</"),this.buf.push(e),this.buf.push(">"))}chars(n){this.buf.push(L_(n))}};function iM(t,n){return(t.compareDocumentPosition(n)&Node.DOCUMENT_POSITION_CONTAINED_BY)!==Node.DOCUMENT_POSITION_CONTAINED_BY}function rM(t){let n=t.nextSibling;if(n&&t!==n.previousSibling)throw Gy(n);return n}function oM(t){let n=t.firstChild;if(n&&iM(t,n))throw Gy(n);return n}function F_(t){let n=t.nodeName;return typeof n=="string"?n:"FORM"}function Gy(t){return new Error(`Failed to sanitize html because the element is clobbered: ${t.outerHTML}`)}var sM=/[\uD800-\uDBFF][\uDC00-\uDFFF]/g,aM=/([^\#-~ |!])/g;function L_(t){return t.replace(/&/g,"&amp;").replace(sM,function(n){let e=n.charCodeAt(0),i=n.charCodeAt(1);return"&#"+((e-55296)*1024+(i-56320)+65536)+";"}).replace(aM,function(n){return"&#"+n.charCodeAt(0)+";"}).replace(/</g,"&lt;").replace(/>/g,"&gt;")}var dd;function dp(t,n){let e=null;try{dd=dd||qI(t);let i=n?String(n):"";e=dd.getInertBodyElement(i);let r=5,o=i;do{if(r===0)throw new Error("Failed to sanitize html because the input is unstable");r--,i=o,o=e.innerHTML,e=dd.getInertBodyElement(i)}while(i!==o);let a=new km().sanitizeChildren(V_(e)||e);return Ad(a)}finally{if(e){let i=V_(e)||e;for(;i.firstChild;)i.firstChild.remove()}}}function V_(t){return"content"in t&&lM(t)?t.content:null}function lM(t){return t.nodeType===Node.ELEMENT_NODE&&t.nodeName==="TEMPLATE"}function cM(t,n){return t.createText(n)}function dM(t,n,e){t.setValue(n,e)}function Wy(t,n,e){return t.createElement(n,e)}function zr(t,n,e,i,r){t.insertBefore(n,e,i,r)}function qy(t,n,e){t.appendChild(n,e)}function B_(t,n,e,i,r){i!==null?zr(t,n,e,i,r):qy(t,n,e)}function Yy(t,n,e,i){t.removeChild(null,n,e,i)}function uM(t,n,e){t.setAttribute(n,"style",e)}function fM(t,n,e){e===""?t.removeAttribute(n,"class"):t.setAttribute(n,"class",e)}function Zy(t,n,e){let{mergedAttrs:i,classes:r,styles:o}=e;i!==null&&xI(t,n,i),r!==null&&fM(t,n,r),o!==null&&uM(t,n,o)}function up(t){let n=hM();return n?n.sanitize(st.URL,t)||"":Hi(t,"URL")?vn(t):Ia(Uc(t))}function hM(){let t=le();return t&&t[un].sanitizer}function fp(t){return t.ownerDocument.defaultView}function mM(t){return t instanceof Function?t():t}function pM(t,n,e){let i=t.length;for(;;){let r=t.indexOf(n,e);if(r===-1)return r;if(r===0||t.charCodeAt(r-1)<=32){let o=n.length;if(r+o===i||t.charCodeAt(r+o)<=32)return r}e=r+1}}var Xy="ng-template";function gM(t,n,e,i){let r=0;if(i){for(;r<n.length&&typeof n[r]=="string";r+=2)if(n[r]==="class"&&pM(n[r+1].toLowerCase(),e,0)!==-1)return!0}else if(hp(t))return!1;if(r=n.indexOf(1,r),r>-1){let o;for(;++r<n.length&&typeof(o=n[r])=="string";)if(o.toLowerCase()===e)return!0}return!1}function hp(t){return t.type===4&&t.value!==Xy}function vM(t,n,e){let i=t.type===4&&!e?Xy:t.value;return n===i}function bM(t,n,e){let i=4,r=t.attrs,o=r!==null?SM(r):0,s=!1;for(let a=0;a<n.length;a++){let l=n[a];if(typeof l=="number"){if(!s&&!mn(i)&&!mn(l))return!1;if(s&&mn(l))continue;s=!1,i=l|i&1;continue}if(!s)if(i&4){if(i=2|i&1,l!==""&&!vM(t,l,e)||l===""&&n.length===1){if(mn(i))return!1;s=!0}}else if(i&8){if(r===null||!gM(t,r,l,e)){if(mn(i))return!1;s=!0}}else{let c=n[++a],u=_M(l,r,hp(t),e);if(u===-1){if(mn(i))return!1;s=!0;continue}if(c!==""){let f;if(u>o?f="":f=r[u+1].toLowerCase(),i&2&&c!==f){if(mn(i))return!1;s=!0}}}}return mn(i)||s}function mn(t){return(t&1)===0}function _M(t,n,e,i){if(n===null)return-1;let r=0;if(i||!e){let o=!1;for(;r<n.length;){let s=n[r];if(s===t)return r;if(s===3||s===6)o=!0;else if(s===1||s===2){let a=n[++r];for(;typeof a=="string";)a=n[++r];continue}else{if(s===4)break;if(s===0){r+=4;continue}}r+=o?1:2}return-1}else return CM(n,t)}function Ky(t,n,e=!1){for(let i=0;i<n.length;i++)if(bM(t,n[i],e))return!0;return!1}function yM(t){let n=t.attrs;if(n!=null){let e=n.indexOf(5);if((e&1)===0)return n[e+1]}return null}function SM(t){for(let n=0;n<t.length;n++){let e=t[n];if(gy(e))return n}return t.length}function CM(t,n){let e=t.indexOf(4);if(e>-1)for(e++;e<t.length;){let i=t[e];if(typeof i=="number")return-1;if(i===n)return e;e++}return-1}function DM(t,n){e:for(let e=0;e<n.length;e++){let i=n[e];if(t.length===i.length){for(let r=0;r<t.length;r++)if(t[r]!==i[r])continue e;return!0}}return!1}function j_(t,n){return t?":not("+n.trim()+")":n}function EM(t){let n=t[0],e=1,i=2,r="",o=!1;for(;e<t.length;){let s=t[e];if(typeof s=="string")if(i&2){let a=t[++e];r+="["+s+(a.length>0?'="'+a+'"':"")+"]"}else i&8?r+="."+s:i&4&&(r+=" "+s);else r!==""&&!mn(s)&&(n+=j_(o,r),r=""),i=s,o=o||!mn(i);e++}return r!==""&&(n+=j_(o,r)),n}function wM(t){return t.map(EM).join(",")}function xM(t){let n=[],e=[],i=1,r=2;for(;i<t.length;){let o=t[i];if(typeof o=="string")r===2?o!==""&&n.push(o,t[++i]):r===8&&e.push(o);else{if(!mn(r))break;r=o}i++}return e.length&&n.push(1,...e),n}var bn={},jn=(function(t){return t[t.Important=1]="Important",t[t.DashCase=2]="DashCase",t})(jn||{}),IM;function mp(t,n){return IM(t,n)}var Bi=new Set;var _3=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var Am=new WeakMap;function Qy(t){return t?t[Ri]??t:null}var va=new WeakSet;function MM(t,n,e){let i=Am.get(t);if(!i||i.length===0)return;let r=n.parentNode,o=n.previousSibling,s=Qy(e);for(let a=i.length-1;a>=0;a--){let{el:l,declarationView:c}=i[a],u=l.parentNode;l===n?(i.splice(a,1),va.add(l),l.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):o&&l===o?(i.splice(a,1),l.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),l.parentNode?.removeChild(l)):u&&r&&u!==r&&(s===null||c===null||s===c)&&(i.splice(a,1),l.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),l.parentNode?.removeChild(l))}}function TM(t,n,e){let i=Qy(e),r=Am.get(t);r?r.some(o=>o.el===n)||r.push({el:n,declarationView:i}):Am.set(t,[{el:n,declarationView:i}])}var Rd=(function(t){return t[t.CHANGE_DETECTION=0]="CHANGE_DETECTION",t[t.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",t})(Rd||{}),Un=new b(""),H_=new Set;function Ui(t){H_.has(t)||(H_.add(t),performance?.mark?.("mark_feature_usage",{detail:{feature:t}}))}var Od=(()=>{class t{impl=null;execute(){this.impl?.execute()}static \u0275prov=G({token:t,providedIn:"root",factory:()=>new t})}return t})(),pp=[0,1,2,3],gp=(()=>{class t{ngZone=d(N);scheduler=d(Rn);errorHandler=d(Ot,{optional:!0});sequences=new Set;deferredRegistrations=new Set;executing=!1;constructor(){d(Un,{optional:!0})}execute(){let e=this.sequences.size>0;e&&Ee(_e.AfterRenderHooksStart),this.executing=!0;for(let i of pp)for(let r of this.sequences)if(!(r.erroredOrDestroyed||!r.hooks[i]))try{r.pipelinedValue=this.ngZone.runOutsideAngular(()=>this.maybeTrace(()=>{let o=r.hooks[i];return o(r.pipelinedValue)},r.snapshot))}catch(o){r.erroredOrDestroyed=!0,this.errorHandler?.handleError(o)}this.executing=!1;for(let i of this.sequences)i.afterRun(),i.once&&(this.sequences.delete(i),i.destroy());for(let i of this.deferredRegistrations)this.sequences.add(i);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear(),e&&Ee(_e.AfterRenderHooksEnd)}register(e){let{view:i}=e;i!==void 0?((i[Fr]??=[]).push(e),jr(i),i[J]|=8192):this.executing?this.deferredRegistrations.add(e):this.addSequence(e)}addSequence(e){this.sequences.add(e),this.scheduler.notify(7)}unregister(e){this.executing&&this.sequences.has(e)?(e.erroredOrDestroyed=!0,e.pipelinedValue=void 0,e.once=!0):(this.sequences.delete(e),this.deferredRegistrations.delete(e))}maybeTrace(e,i){return i?i.run(Rd.AFTER_NEXT_RENDER,e):e()}static \u0275prov=G({token:t,providedIn:"root",factory:()=>new t})}return t})(),Sa=class{impl;hooks;view;once;snapshot;erroredOrDestroyed=!1;pipelinedValue=void 0;unregisterOnDestroy;constructor(n,e,i,r,o,s=null){this.impl=n,this.hooks=e,this.view=i,this.once=r,this.snapshot=s,this.unregisterOnDestroy=o?.onDestroy(()=>this.destroy())}afterRun(){this.erroredOrDestroyed=!1,this.pipelinedValue=void 0,this.snapshot?.dispose(),this.snapshot=null}destroy(){this.impl.unregister(this),this.unregisterOnDestroy?.();let n=this.view?.[Fr];n&&(this.view[Fr]=n.filter(e=>e!==this))}};function Qe(t,n){let e=n?.injector??d(R);return Ui("NgAfterNextRender"),kM(t,e,n,!0)}function NM(t){return t instanceof Function?[void 0,void 0,t,void 0]:[t.earlyRead,t.write,t.mixedReadWrite,t.read]}function kM(t,n,e,i){let r=n.get(Od);r.impl??=n.get(gp);let o=n.get(Un,null,{optional:!0}),s=e?.manualCleanup!==!0?n.get(We):null,a=n.get(jo,null,{optional:!0}),l=new Sa(r.impl,NM(t),a?.view,i,s,o?.snapshot(null));return r.impl.register(l),l}var vp=new b("",{factory:()=>{let t=d(Ie),n=new Set;return t.onDestroy(()=>n.clear()),{queue:n,isScheduled:!1,scheduler:null,injector:t}}});function Jy(t,n,e){let i=t.get(vp);if(Array.isArray(n))for(let r of n)i.queue.add(r),e?.detachedLeaveAnimationFns?.push(r);else i.queue.add(n),e?.detachedLeaveAnimationFns?.push(n);i.scheduler&&i.scheduler(t)}function AM(t,n){let e=t.get(vp);if(Array.isArray(n))for(let i of n)e.queue.delete(i);else e.queue.delete(n)}function RM(t,n){let e=t.get(vp);if(n.detachedLeaveAnimationFns){for(let i of n.detachedLeaveAnimationFns)e.queue.delete(i);n.detachedLeaveAnimationFns=void 0}}function OM(t,n){for(let[e,i]of n)Jy(t,i.animateFns)}function U_(t,n,e,i){let r=t?.[Vn]?.enter;n!==null&&r&&r.has(e.index)&&OM(i,r)}function z_(t,n,e,i){try{e.get(ca)}catch{return i(!1)}let r=t?.[Vn];r?.enter?.has(n.index)&&AM(e,r.enter.get(n.index).animateFns);let o=PM(t,n,r);if(o.size===0){let s=!1;if(t){let a=[];Pd(t,n,a),s=a.length>0}if(!s)return i(!1)}t&&Bi.add(t[Ln]),Jy(e,()=>FM(t,n,r||void 0,o,i),r||void 0)}function PM(t,n,e){let i=new Map,r=e?.leave;if(r&&r.has(n.index)&&i.set(n.index,r.get(n.index)),t&&r)for(let[o,s]of r){if(i.has(o))continue;let l=t[Z].data[o].parent;for(;l;){if(l===n){i.set(o,s);break}l=l.parent}}return i}function FM(t,n,e,i,r){let o=[];if(e&&e.leave)for(let[s]of i){if(!e.leave.has(s))continue;let a=e.leave.get(s);for(let l of a.animateFns){let{promise:c}=l();o.push(c)}e.detachedLeaveAnimationFns=void 0}if(t&&Pd(t,n,o),o.length>0){let s=e||t?.[Vn];if(s){let a=s.running;a&&o.push(a),s.running=Promise.allSettled(o),VM(t,s.running,r)}else Promise.allSettled(o).then(()=>{t&&Bi.delete(t[Ln]),r(!0)})}else t&&Bi.delete(t[Ln]),r(!1)}function Pd(t,n,e){if(n.type&12){let r=t[n.index];if(Wt(r))for(let o=je;o<r.length;o++){let s=r[o];s[Z].type===2&&LM(s,e)}}let i=n.child;for(;i;)Pd(t,i,e),i=i.next}function LM(t,n){let e=t[Vn];if(e&&e.leave)for(let r of e.leave.values())for(let o of r.animateFns){let{promise:s}=o();n.push(s)}let i=t[Z].firstChild;for(;i;)Pd(t,i,n),i=i.next}function VM(t,n,e){n.then(()=>{t[Vn]?.running===n&&(t[Vn].running=void 0,Bi.delete(t[Ln])),e(!0)})}function Ho(t,n,e,i,r,o,s,a){if(r!=null){let l,c=!1;Wt(r)?l=r:ni(r)&&(c=!0,r=r[dn]);let u=gt(r);t===0&&i!==null?(U_(a,i,o,e),s==null?qy(n,i,u):zr(n,i,u,s||null,!0)):t===1&&i!==null?(U_(a,i,o,e),zr(n,i,u,s||null,!0),MM(o,u,a)):t===2?(a?.[Vn]?.leave?.has(o.index)&&TM(o,u,a),va.delete(u),z_(a,o,e,f=>{if(va.has(u)){va.delete(u);return}Yy(n,u,c,f)})):t===3&&(va.delete(u),z_(a,o,e,()=>{n.destroyNode(u)})),l!=null&&ZM(n,t,e,l,o,i,s)}}function BM(t,n){eS(t,n),n[dn]=null,n[pt]=null}function jM(t,n,e,i,r,o){i[dn]=r,i[pt]=n,Ld(t,i,e,1,r,o)}function eS(t,n){n[un].changeDetectionScheduler?.notify(9),Ld(t,n,n[Fe],2,null,null)}function HM(t){let n=t[Lo];if(!n)return cm(t[Z],t);for(;n;){let e=null;if(ni(n))e=n[Lo];else{let i=n[je];i&&(e=i)}if(!e){for(;n&&!n[Gt]&&n!==t;)ni(n)&&cm(n[Z],n),n=n[rt];n===null&&(n=t),ni(n)&&cm(n[Z],n),e=n&&n[Gt]}n=e}}function bp(t,n){let e=t[Vr],i=e.indexOf(n);e.splice(i,1)}function Fd(t,n){if(Br(n))return;let e=n[Fe];e.destroyNode&&Ld(t,n,e,3,null,null),HM(n)}function cm(t,n){if(Br(n))return;let e=K(null);try{n[J]&=-129,n[J]|=256,n[Bt]&&Mi(n[Bt]),zM(t,n),UM(t,n),n[Z].type===1&&n[Fe].destroy();let i=n[Oi];if(i!==null&&Wt(n[rt])){i!==n[rt]&&bp(i,n);let r=n[Fn];r!==null&&r.detachView(t)}Sm(n)}finally{K(e)}}function UM(t,n){let e=t.cleanup,i=n[Fo];if(e!==null)for(let s=0;s<e.length-1;s+=2)if(typeof e[s]=="string"){let a=e[s+3];a>=0?i[a]():i[-a].unsubscribe(),s+=2}else{let a=i[e[s+1]];e[s].call(a)}i!==null&&(n[Fo]=null);let r=n[Qn];if(r!==null){n[Qn]=null;for(let s=0;s<r.length;s++){let a=r[s];a()}}let o=n[Jn];if(o!==null){n[Jn]=null;for(let s of o)s.destroy()}}function zM(t,n){let e;if(t!=null&&(e=t.destroyHooks)!=null)for(let i=0;i<e.length;i+=2){let r=n[e[i]];if(!(r instanceof $r)){let o=e[i+1];if(Array.isArray(o))for(let s=0;s<o.length;s+=2){let a=r[o[s]],l=o[s+1];Ee(_e.LifecycleHookStart,a,l);try{l.call(a)}finally{Ee(_e.LifecycleHookEnd,a,l)}}else{Ee(_e.LifecycleHookStart,r,o);try{o.call(r)}finally{Ee(_e.LifecycleHookEnd,r,o)}}}}}function tS(t,n,e){if(n===null)throw new x(510,!1);return $M(t,n.parent,e)}function $M(t,n,e){let i=n;for(;i!==null&&i.type&168;)n=i,i=n.parent;if(i===null)return e[dn];if(ii(i)){let{encapsulation:r}=t.data[i.directiveStart+i.componentOffset];if(r===gn.None||r===gn.Emulated)return null}return hn(i,e)}function nS(t,n,e){return WM(t,n,e)}function GM(t,n,e){return t.type&40?hn(t,e):null}var WM=GM,$_;function _p(t,n,e,i){let r=tS(t,i,n),o=n[Fe],s=i.parent||n[pt],a=nS(s,i,n);if(r!=null)if(Array.isArray(e))for(let l=0;l<e.length;l++)B_(o,r,e[l],a,!1);else B_(o,r,e,a,!1);$_!==void 0&&$_(o,i,n,e,r)}function ba(t,n){if(n!==null){let e=n.type;if(e&3)return hn(n,t);if(e&4)return Rm(-1,t[n.index]);if(e&8){let i=n.child;if(i!==null)return ba(t,i);{let r=t[n.index];return Wt(r)?Rm(-1,r):gt(r)}}else{if(e&128)return ba(t,n.next);if(e&32)return mp(n,t)()||gt(t[n.index]);{let i=iS(t,n);if(i!==null){if(Array.isArray(i))return i[0];let r=ei(t[kt]);return ba(r,i)}else return ba(t,n.next)}}}return null}function iS(t,n){if(n!==null){let i=t[kt][pt],r=n.projection;return i.projection[r]}return null}function Rm(t,n){let e=je+t+1;if(e<n.length){let i=n[e],r=i[Z].firstChild;if(r!==null)return ba(i,r)}return n[Lr]}function yp(t,n,e,i,r,o,s){for(;e!=null;){let a=i[Pn];if(e.type===128){e=e.next;continue}let l=i[e.index],c=e.type;if(s&&n===0&&(l&&$o(gt(l),i),e.flags|=2),!kd(e))if(c&8)yp(t,n,e.child,i,r,o,!1),Ho(n,t,a,r,l,e,o,i);else if(c&32){let u=mp(e,i),f;for(;f=u();)Ho(n,t,a,r,f,e,o,i);Ho(n,t,a,r,l,e,o,i)}else c&16?rS(t,n,i,e,r,o):Ho(n,t,a,r,l,e,o,i);e=s?e.projectionNext:e.next}}function Ld(t,n,e,i,r,o){t.type===3?qM(e,i,n,r,o):yp(e,i,t.firstChild,n,r,o,!1)}function qM(t,n,e,i,r){let s=e[Z].firstChild,a=s.next,l=gt(e[s.index]),c=gt(e[a.index]),u=a.index+1,f=e[u];if(n===1||n===0)i!==null&&(f&&f.hasChildNodes()?zr(t,i,f,r,!0):(zr(t,i,l,r,!0),zr(t,i,c,r,!0)));else if(n===2){if(f||(f=document.createDocumentFragment(),e[u]=f),l&&l.parentNode===f)return;let h=l;for(;h!==null;){let p=h.nextSibling;if(f.appendChild(h),h===c)break;h=p}}}function YM(t,n,e){let i=n[Fe],r=tS(t,e,n),o=e.parent||n[pt],s=nS(o,e,n);rS(i,0,n,e,r,s)}function rS(t,n,e,i,r,o){let s=e[kt],l=s[pt].projection[i.projection];if(Array.isArray(l))for(let c=0;c<l.length;c++){let u=l[c];Ho(n,t,e[Pn],r,u,i,o,e)}else{let c=l,u=s[rt];Iy(i)&&(c.flags|=128),yp(t,n,c,u,r,o,!0)}}function ZM(t,n,e,i,r,o,s){let a=i[Lr],l=gt(i);if(a!==l&&Ho(n,t,e,o,a,r,s),(i[J]&4)===0)for(let c=je;c<i.length;c++){let u=i[c];Ld(u[Z],u,t,n,o,a)}}function XM(t,n,e,i,r){if(n)r?t.addClass(e,i):t.removeClass(e,i);else{let o=i.indexOf("-")===-1?void 0:jn.DashCase;r==null?t.removeStyle(e,i,o):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),o|=jn.Important),t.setStyle(e,i,r,o))}}function Sp(t,n,e,i,r,o,s,a,l,c,u){let f=Ye+i,h=f+r,p=KM(f,h),S=typeof c=="function"?c():c;return p[Z]={type:t,blueprint:p,template:e,queries:null,viewQuery:a,declTNode:n,data:p.slice().fill(null,f),bindingStartIndex:f,expandoStartIndex:h,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof o=="function"?o():o,pipeRegistry:typeof s=="function"?s():s,firstChild:null,schemas:l,consts:S,incompleteFirstPass:!1,ssrId:u}}function KM(t,n){let e=[];for(let i=0;i<n;i++)e.push(i<t?null:bn);return e}function QM(t){let n=t.tView;return n===null||n.incompleteFirstPass?t.tView=Sp(1,null,t.template,t.decls,t.vars,t.directiveDefs,t.pipeDefs,t.viewQuery,t.schemas,t.consts,t.id):n}function Cp(t,n,e,i,r,o,s,a,l,c,u){let f=n.blueprint.slice();return f[dn]=r,f[J]=i|4|128|8|64|1024,(c!==null||t&&t[J]&2048)&&(f[J]|=2048),Vh(f),f[rt]=f[Ri]=t,f[qe]=e,f[un]=s||t&&t[un],f[Fe]=a||t&&t[Fe],f[Pn]=l||t&&t[Pn]||null,f[pt]=o,f[Ln]=jI(),f[Or]=u,f[Rh]=c,f[kt]=n.type==2?t[kt]:f,f}function JM(t,n,e){let i=hn(n,t),r=QM(e),o=t[un].rendererFactory,s=Dp(t,Cp(t,r,null,oS(e),i,n,null,o.createRenderer(i,e),null,null,null));return t[n.index]=s}function oS(t){let n=16;return t.signals?n=4096:t.onPush&&(n=64),n}function sS(t,n,e,i){if(e===0)return-1;let r=n.length;for(let o=0;o<e;o++)n.push(i),t.blueprint.push(i),t.data.push(null);return r}function Dp(t,n){return t[Lo]?t[Ah][Gt]=n:t[Lo]=n,t[Ah]=n,n}function _(t=1){aS(He(),le(),ri()+t,!1)}function aS(t,n,e,i){if(!i)if((n[J]&3)===3){let o=t.preOrderCheckHooks;o!==null&&fd(n,o,e)}else{let o=t.preOrderHooks;o!==null&&hd(n,o,0,e)}Pi(e)}var Vd=(function(t){return t[t.None=0]="None",t[t.SignalBased=1]="SignalBased",t[t.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",t})(Vd||{});function Om(t,n,e,i){let r=K(null);try{let[o,s,a]=t.inputs[e],l=null;(s&Vd.SignalBased)!==0&&(l=n[o][et]),l!==null&&l.transformFn!==void 0?i=l.transformFn(i):a!==null&&(i=a.call(n,i)),t.setInput!==null?t.setInput(n,l,i,e,o):uy(n,l,o,i)}finally{K(r)}}function lS(t,n,e,i,r){let o=ri(),s=i&2;try{Pi(-1),s&&n.length>Ye&&aS(t,n,Ye,!1);let a=s?_e.TemplateUpdateStart:_e.TemplateCreateStart;Ee(a,r,e),e(i,r)}finally{Pi(o);let a=s?_e.TemplateUpdateEnd:_e.TemplateCreateEnd;Ee(a,r,e)}}function Ep(t,n,e){sT(t,n,e),(e.flags&64)===64&&aT(t,n,e)}function Bd(t,n,e=hn){let i=n.localNames;if(i!==null){let r=n.index+1;for(let o=0;o<i.length;o+=2){let s=i[o+1],a=s===-1?e(n,t):t[s];t[r++]=a}}}function eT(t,n,e,i){let o=i.get(Fy,Py)||e===gn.ShadowDom||e===gn.ExperimentalIsolatedShadowDom,s=t.selectRootElement(n,o);return tT(s),s}function tT(t){nT(t)}var nT=()=>null;function iT(t){return t==="class"?"className":t==="for"?"htmlFor":t==="formaction"?"formAction":t==="innerHtml"?"innerHTML":t==="readonly"?"readOnly":t==="tabindex"?"tabIndex":t}function rT(t,n,e,i,r,o){let s=n[Z];if(wp(t,s,n,e,i)){ii(t)&&oT(n,t.index);return}t.type&3&&(e=iT(e)),cS(t,n,e,i,r,o)}function cS(t,n,e,i,r,o){if(t.type&3){let s=hn(t,n);i=o!=null?o(i,t.value||"",e):i,r.setProperty(s,e,i)}else t.type&12}function oT(t,n){let e=qt(n,t);e[J]&16||(e[J]|=64)}function sT(t,n,e){let i=e.directiveStart,r=e.directiveEnd;ii(e)&&JM(n,e,t.data[i+e.componentOffset]),t.firstCreatePass||yd(e,n);let o=e.initialInputs;for(let s=i;s<r;s++){let a=t.data[s],l=ya(n,t,s,e);if($o(l,n),o!==null&&uT(n,s-i,l,a,e,o),fn(a)){let c=qt(e.index,n);c[qe]=ya(n,t,s,e)}}}function aT(t,n,e){let i=e.directiveStart,r=e.directiveEnd,o=e.index,s=d_();try{Pi(o);for(let a=i;a<r;a++){let l=t.data[a],c=n[a];Qc(a),(l.hostBindings!==null||l.hostVars!==0||l.hostAttrs!==null)&&lT(l,c)}}finally{Pi(-1),Qc(s)}}function lT(t,n){t.hostBindings!==null&&t.hostBindings(1,n)}function dS(t,n){let e=t.directiveRegistry,i=null;if(e)for(let r=0;r<e.length;r++){let o=e[r];Ky(n,o.selectors,!1)&&(i??=[],fn(o)?i.unshift(o):i.push(o))}return i}function cT(t,n,e,i,r,o){let s=hn(t,n);dT(n[Fe],s,o,t.value,e,i,r)}function dT(t,n,e,i,r,o,s){if(o==null)s?.(o,i||"",r),t.removeAttribute(n,r,e);else{let a=s==null?Uc(o):s(o,i||"",r);t.setAttribute(n,r,a,e)}}function uT(t,n,e,i,r,o){let s=o[n];if(s!==null)for(let a=0;a<s.length;a+=2){let l=s[a],c=s[a+1];Om(i,e,l,c)}}function uS(t,n,e,i,r){let o=Ye+e,s=n[Z],a=r(s,n,t,i,e);n[o]=a,Bo(t,!0);let l=t.type===2;return l?(Zy(n[Fe],a,t),(i_()===0||fa(t))&&$o(a,n),r_()):$o(a,n),rd()&&(!l||!kd(t))&&_p(s,n,a,t),t}function fS(t){let n=t;return Yh()?Zh():(n=n.parent,Bo(n,!1)),n}function fT(t,n){let e=t[Pn];if(!e)return;let i;try{i=e.get(Yt,null)}catch{i=null}i?.(n)}function wp(t,n,e,i,r){let o=t.inputs?.[i],s=t.hostDirectiveInputs?.[i],a=!1;if(s)for(let l=0;l<s.length;l+=2){let c=s[l],u=s[l+1],f=n.data[c];Om(f,e[c],u,r),a=!0}if(o)for(let l of o){let c=e[l],u=n.data[l];Om(u,c,i,r),a=!0}return a}function hT(t,n){let e=qt(n,t),i=e[Z];mT(i,e);let r=e[dn];r!==null&&e[Or]===null&&(e[Or]=Ly(r,e[Pn])),Ee(_e.ComponentStart);try{xp(i,e,e[qe])}finally{Ee(_e.ComponentEnd,e[qe])}}function mT(t,n){for(let e=n.length;e<t.blueprint.length;e++)n.push(t.blueprint[e])}function xp(t,n,e){ed(n);try{let i=t.viewQuery;i!==null&&Dm(1,i,e);let r=t.template;r!==null&&lS(t,n,r,1,e),t.firstCreatePass&&(t.firstCreatePass=!1),n[Fn]?.finishViewCreation(t),t.staticContentQueries&&Vy(t,n),t.staticViewQueries&&Dm(2,t.viewQuery,e);let o=t.components;o!==null&&pT(n,o)}catch(i){throw t.firstCreatePass&&(t.incompleteFirstPass=!0,t.firstCreatePass=!1),i}finally{n[J]&=-5,td()}}function pT(t,n){for(let e=0;e<n.length;e++)hT(t,n[e])}function Ta(t,n,e,i){let r=K(null);try{let o=n.tView,a=t[J]&4096?4096:16,l=Cp(t,o,e,a,null,n,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),c=t[n.index];l[Oi]=c;let u=t[Fn];return u!==null&&(l[Fn]=u.createEmbeddedView(o)),xp(o,l,e),l}finally{K(r)}}function Go(t,n){return!n||n.firstChild===null||Iy(t)}function Ca(t,n,e,i,r=!1){if(t.type===3){let o=t.firstChild,s=o.next,a=gt(n[o.index]),l=gt(n[s.index]),c=a;for(;c!==null&&(i.push(c),c!==l);)c=c.nextSibling;return i}for(;e!==null;){if(e.type===128){e=r?e.projectionNext:e.next;continue}let o=n[e.index];if(o!==null)if(Wt(o)){let a=o[Lr];a!==o[dn]&&i.push(gt(o)),o[J]&4||hS(o,i),i.push(a)}else i.push(gt(o));let s=e.type;if(s&8)Ca(t,n,e.child,i);else if(s&32){let a=mp(e,n),l;for(;l=a();)i.push(l)}else if(s&16){let a=iS(n,e);if(Array.isArray(a))i.push(...a);else{let l=ei(n[kt]);Ca(l[Z],l,a,i,!0)}}e=r?e.projectionNext:e.next}return i}function hS(t,n){for(let e=je;e<t.length;e++){let i=t[e],r=i[Z].firstChild;r!==null&&Ca(i[Z],i,r,n)}}function mS(t){if(t[Fr]!==null){for(let n of t[Fr])n.impl.addSequence(n);t[Fr].length=0}}var pS=[];function gT(t){return t[Bt]??vT(t)}function vT(t){let n=pS.pop()??Object.create(_T);return n.lView=t,n}function bT(t){t.lView[Bt]!==t&&(t.lView=null,pS.push(t))}var _T=k(v({},gr),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{jr(t.lView)},consumerOnSignalRead(){this.lView[Bt]=this}});function yT(t){let n=t[Bt]??Object.create(ST);return n.lView=t,n}var ST=k(v({},gr),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{let n=ei(t.lView);for(;n&&!gS(n[Z]);)n=ei(n);n&&Bh(n)},consumerOnSignalRead(){this.lView[Bt]=this}});function gS(t){return t.type!==2}function vS(t){if(t[Jn]===null)return;let n=!0;for(;n;){let e=!1;for(let i of t[Jn])if(i.dirty&&(e=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()),t[Jn]===null))return;n=e&&!!(t[J]&8192)}}var CT=100;function bS(t,n=0){let i=t[un].rendererFactory,r=!1;r||i.begin?.();try{DT(t,n)}finally{r||i.end?.()}}function DT(t,n){let e=Xh();try{ta(!0),Pm(t,n);let i=0;for(;ha(t);){if(i===CT)throw new x(103,!1);i++,Pm(t,1)}}finally{ta(e)}}function ET(t,n,e,i){if(Br(n))return;let r=n[J],o=!1,s=!1;ed(n);let a=!0,l=null,c=null;o||(gS(t)?(c=gT(n),l=Ii(c)):Yl()===null?(a=!1,c=yT(n),l=Ii(c)):n[Bt]&&(Mi(n[Bt]),n[Bt]=null));try{Vh(n),a_(t.bindingStartIndex),e!==null&&lS(t,n,e,2,i);let u=(r&3)===3;if(!o)if(u){let p=t.preOrderCheckHooks;p!==null&&fd(n,p,null)}else{let p=t.preOrderHooks;p!==null&&hd(n,p,0,null),am(n,0)}if(s||wT(n),vS(n),_S(n,0),t.contentQueries!==null&&Vy(t,n),!o)if(u){let p=t.contentCheckHooks;p!==null&&fd(n,p)}else{let p=t.contentHooks;p!==null&&hd(n,p,1),am(n,1)}IT(t,n);let f=t.components;f!==null&&SS(n,f,0);let h=t.viewQuery;if(h!==null&&Dm(2,h,i),!o)if(u){let p=t.viewCheckHooks;p!==null&&fd(n,p)}else{let p=t.viewHooks;p!==null&&hd(n,p,2),am(n,2)}if(t.firstUpdatePass===!0&&(t.firstUpdatePass=!1),n[qc]){for(let p of n[qc])p();n[qc]=null}o||(mS(n),n[J]&=-73)}catch(u){throw o||jr(n),u}finally{c!==null&&(br(c,l),a&&bT(c)),td()}}function _S(t,n){for(let e=Ty(t);e!==null;e=Ny(e))for(let i=je;i<e.length;i++){let r=e[i];yS(r,n)}}function wT(t){for(let n=Ty(t);n!==null;n=Ny(n)){if(!(n[J]&2))continue;let e=n[Vr];for(let i=0;i<e.length;i++){let r=e[i];Bh(r)}}}function xT(t,n,e){Ee(_e.ComponentStart);let i=qt(n,t);try{yS(i,e)}finally{Ee(_e.ComponentEnd,i[qe])}}function yS(t,n){Zc(t)&&Pm(t,n)}function Pm(t,n){let i=t[Z],r=t[J],o=t[Bt],s=!!(n===0&&r&16);if(s||=!!(r&64&&n===0),s||=!!(r&1024),s||=!!(o?.dirty&&Co(o)),s||=!1,o&&(o.dirty=!1),t[J]&=-9217,s)ET(i,t,i.template,t[qe]);else if(r&8192){let a=K(null);try{vS(t),_S(t,1);let l=i.components;l!==null&&SS(t,l,1),mS(t)}finally{K(a)}}}function SS(t,n,e){for(let i=0;i<n.length;i++)xT(t,n[i],e)}function IT(t,n){let e=t.hostBindingOpCodes;if(e!==null)try{for(let i=0;i<e.length;i++){let r=e[i];if(r<0)Pi(~r);else{let o=r,s=e[++i],a=e[++i];c_(s,o);let l=n[o];Ee(_e.HostBindingsUpdateStart,l);try{a(2,l)}finally{Ee(_e.HostBindingsUpdateEnd,l)}}}}finally{Pi(-1)}}function Ip(t,n){let e=Xh()?64:1088;for(t[un].changeDetectionScheduler?.notify(n);t;){t[J]|=e;let i=ei(t);if(Vo(t)&&!i)return t;t=i}return null}function CS(t,n,e,i){return[t,!0,0,n,null,i,null,e,null,null]}function DS(t,n){let e=je+n;if(e<t.length)return t[e]}function Na(t,n,e,i=!0){let r=n[Z];if(MT(r,n,t,e),i){let s=Rm(e,t),a=n[Fe],l=a.parentNode(t[Lr]);l!==null&&jM(r,t[pt],a,n,l,s)}let o=n[Or];o!==null&&o.firstChild!==null&&(o.firstChild=null)}function ES(t,n){let e=Da(t,n);return e!==void 0&&Fd(e[Z],e),e}function Da(t,n){if(t.length<=je)return;let e=je+n,i=t[e];if(i){let r=i[Oi];r!==null&&r!==t&&bp(r,i),n>0&&(t[e-1][Gt]=i[Gt]);let o=la(t,je+n);BM(i[Z],i);let s=o[Fn];s!==null&&s.detachView(o[Z]),i[rt]=null,i[Gt]=null,i[J]&=-129}return i}function MT(t,n,e,i){let r=je+i,o=e.length;i>0&&(e[r-1][Gt]=n),i<o-je?(n[Gt]=e[r],Ih(e,je+i,n)):(e.push(n),n[Gt]=null),n[rt]=e;let s=n[Oi];s!==null&&e!==s&&wS(s,n);let a=n[Fn];a!==null&&a.insertView(t),Xc(n),n[J]|=128}function wS(t,n){let e=t[Vr],i=n[rt];if(ni(i))t[J]|=2;else{let r=i[rt][kt];n[kt]!==r&&(t[J]|=2)}e===null?t[Vr]=[n]:e.push(n)}var ji=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let n=this._lView,e=n[Z];return Ca(e,n,e.firstChild,[])}constructor(n,e){this._lView=n,this._cdRefInjectingView=e}get context(){return this._lView[qe]}set context(n){this._lView[qe]=n}get destroyed(){return Br(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let n=this._lView[rt];if(Wt(n)){let e=n[ua],i=e?e.indexOf(this):-1;i>-1&&(Da(n,i),la(e,i))}this._attachedToViewContainer=!1}Fd(this._lView[Z],this._lView)}onDestroy(n){Kc(this._lView,n)}markForCheck(){Ip(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[J]&=-129}reattach(){Xc(this._lView),this._lView[J]|=128}detectChanges(){this._lView[J]|=1024,bS(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new x(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let n=Vo(this._lView),e=this._lView[Oi];e!==null&&!n&&bp(e,this._lView),eS(this._lView[Z],this._lView)}attachToAppRef(n){if(this._attachedToViewContainer)throw new x(902,!1);this._appRef=n;let e=Vo(this._lView),i=this._lView[Oi];i!==null&&!e&&wS(i,this._lView),Xc(this._lView)}};var Ct=(()=>{class t{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=TT;constructor(e,i,r){this._declarationLView=e,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(e,i){return this.createEmbeddedViewImpl(e,i)}createEmbeddedViewImpl(e,i,r){let o=Ta(this._declarationLView,this._declarationTContainer,e,{embeddedViewInjector:i,dehydratedView:r});return new ji(o)}}return t})();function TT(){return jd(vt(),le())}function jd(t,n){return t.type&4?new Ct(n,t,Zo(t,n)):null}function Xo(t,n,e,i,r){let o=t.data[n];if(o===null)o=NT(t,n,e,i,r),l_()&&(o.flags|=32);else if(o.type&64){o.type=e,o.value=i,o.attrs=r;let s=o_();o.injectorIndex=s===null?-1:s.injectorIndex}return Bo(o,!0),o}function NT(t,n,e,i,r){let o=qh(),s=Yh(),a=s?o:o&&o.parent,l=t.data[n]=AT(t,a,e,n,i,r);return kT(t,l,o,s),l}function kT(t,n,e,i){t.firstChild===null&&(t.firstChild=n),e!==null&&(i?e.child==null&&n.parent!==null&&(e.child=n):e.next===null&&(e.next=n,n.prev=e))}function AT(t,n,e,i,r,o){let s=n?n.injectorIndex:-1,a=0;return $h()&&(a|=128),{type:e,index:i,insertBeforeIndex:null,injectorIndex:s,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:a,providerIndexes:0,value:r,namespace:em(),attrs:o,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:n,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}function RT(t){let n=t[Oh]??[],i=t[rt][Fe],r=[];for(let o of n)o.data[Oy]!==void 0?r.push(o):OT(o,i);t[Oh]=r}function OT(t,n){let e=0,i=t.firstChild;if(i){let r=t.data[Ry];for(;e<r;){let o=i.nextSibling;Yy(n,i,!1),i=o,e++}}}var PT=()=>null,FT=()=>null;function Sd(t,n){return PT(t,n)}function xS(t,n,e){return FT(t,n,e)}var IS=class{},Ke=class{},Re=class{destroyNode=null;static __NG_ELEMENT_ID__=()=>LT()};function LT(){let t=le(),n=vt(),e=qt(n.index,t);return(ni(e)?e:t)[Fe]}var MS=(()=>{class t{static \u0275prov=G({token:t,providedIn:"root",factory:()=>null})}return t})();function TS(t){return t.debugInfo?.className||t.type.name||null}var pd={},Cd=class{injector;parentInjector;constructor(n,e){this.injector=n,this.parentInjector=e}get(n,e,i){let r=this.injector.get(n,pd,i);return r!==pd||e===pd?r:this.parentInjector.get(n,e,i)}};function zi(t,n,e){if(e===bn)return!1;let i=t[n];return Object.is(i,e)?!1:(t[n]=e,!0)}function gd(t,n,e){return function i(r){let o=i.__ngNativeEl__;o!==void 0&&UI(r,o);let s=ii(t)?qt(t.index,n):n;Ip(s,5);let a=n[qe],l=G_(n,a,e,r),c=i.__ngNextListenerFn__;for(;c;)l=G_(n,a,c,r)&&l,c=c.__ngNextListenerFn__;return l}}function G_(t,n,e,i){let r=K(null);try{return Ee(_e.OutputStart,n,e),e(i)!==!1}catch(o){return fT(t,o),!1}finally{Ee(_e.OutputEnd,n,e),K(r)}}function NS(t,n,e,i,r,o,s,a){let l=fa(t),c=!1,u=null;if(!i&&l&&(u=BT(n,e,o,t.index)),u!==null){let f=u.__ngLastListenerFn__||u;f.__ngNextListenerFn__=s,u.__ngLastListenerFn__=s,c=!0}else{let f=hn(t,e),h=i?i(f):f;$I(e,h,o,a),i||(a.__ngNativeEl__=f);let p=r.listen(h,o,a);if(!VT(o)){let S=i?C=>i(gt(C[t.index])):t.index;kS(S,n,e,o,a,p,!1)}}return c}function VT(t){return t.startsWith("animation")||t.startsWith("transition")}function BT(t,n,e,i){let r=t.cleanup;if(r!=null)for(let o=0;o<r.length-1;o+=2){let s=r[o];if(s===e&&r[o+1]===i){let a=n[Fo],l=r[o+2];return a&&a.length>l?a[l]:null}typeof s=="string"&&(o+=2)}return null}function kS(t,n,e,i,r,o,s){let a=n.firstCreatePass?Hh(n):null,l=jh(e),c=l.length;l.push(r,o),a&&a.push(i,t,c,(c+1)*(s?-1:1))}function W_(t,n,e,i,r,o){let s=n[e],a=n[Z],c=a.data[e].outputs[i],f=s[c].subscribe(o);kS(t.index,a,n,r,o,f,!0)}var Fm=Symbol("BINDING");var Wr=new b("");function Dd(t,n,e){let i=e?t.styles:null,r=e?t.classes:null,o=0;if(n!==null)for(let s=0;s<n.length;s++){let a=n[s];if(typeof a=="number")o=a;else if(o==1)r=Vc(r,a);else if(o==2){let l=a,c=n[++s];i=Vc(i,l+": "+c+";")}}e?t.styles=i:t.stylesWithoutHost=i,e?t.classes=r:t.classesWithoutHost=r}function ft(t,n=0){let e=le();if(e===null)return A(t,n);let i=vt();return Dy(i,e,it(t),n)}function Hd(){let t="invalid";throw new Error(t)}function AS(t,n,e,i,r){let o=i===null?null:{"":-1},s=r(t,e);if(s!==null){let a=s,l=null,c=null;for(let u of s)if(u.resolveHostDirectives!==null){[a,l,c]=u.resolveHostDirectives(s);break}UT(t,n,e,a,o,l,c)}o!==null&&i!==null&&jT(e,i,o)}function jT(t,n,e){let i=t.localNames=[];for(let r=0;r<n.length;r+=2){let o=e[n[r+1]];if(o==null)throw new x(-301,!1);i.push(n[r],o)}}function HT(t,n,e){n.componentOffset=e,(t.components??=[]).push(n.index)}function UT(t,n,e,i,r,o,s){let a=i.length,l=null;for(let h=0;h<a;h++){let p=i[h];l===null&&fn(p)&&(l=p,HT(t,e,h)),_m(yd(e,n),t,p.type)}YT(e,t.data.length,a),l?.viewProvidersResolver&&l.viewProvidersResolver(l);for(let h=0;h<a;h++){let p=i[h];p.providersResolver&&p.providersResolver(p)}let c=!1,u=!1,f=sS(t,n,a,null);a>0&&(e.directiveToIndex=new Map);for(let h=0;h<a;h++){let p=i[h];if(e.mergedAttrs=zo(e.mergedAttrs,p.hostAttrs),$T(t,e,n,f,p),qT(f,p,r),s!==null&&s.has(p)){let[C,$]=s.get(p);e.directiveToIndex.set(p.type,[f,C+e.directiveStart,$+e.directiveStart])}else(o===null||!o.has(p))&&e.directiveToIndex.set(p.type,f);p.contentQueries!==null&&(e.flags|=4),(p.hostBindings!==null||p.hostAttrs!==null||p.hostVars!==0)&&(e.flags|=64);let S=p.type.prototype;!c&&(S.ngOnChanges||S.ngOnInit||S.ngDoCheck)&&((t.preOrderHooks??=[]).push(e.index),c=!0),!u&&(S.ngOnChanges||S.ngDoCheck)&&((t.preOrderCheckHooks??=[]).push(e.index),u=!0),f++}zT(t,e,o)}function zT(t,n,e){for(let i=n.directiveStart;i<n.directiveEnd;i++){let r=t.data[i];if(e===null||!e.has(r))q_(0,n,r,i),q_(1,n,r,i),Z_(n,i,!1);else{let o=e.get(r);Y_(0,n,o,i),Y_(1,n,o,i),Z_(n,i,!0)}}}function q_(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(Object.hasOwn(r,o)){let s;t===0?s=n.inputs??={}:s=n.outputs??={},s[o]??=[],s[o].push(i),RS(n,o)}}function Y_(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(Object.hasOwn(r,o)){let s=r[o],a;t===0?a=n.hostDirectiveInputs??={}:a=n.hostDirectiveOutputs??={},a[s]??=[],a[s].push(i,o),RS(n,s)}}function RS(t,n){n==="class"?t.flags|=8:n==="style"&&(t.flags|=16)}function Z_(t,n,e){let{attrs:i,inputs:r,hostDirectiveInputs:o}=t;if(i===null||!e&&r===null||e&&o===null||hp(t)){t.initialInputs??=[],t.initialInputs.push(null);return}let s=null,a=0;for(;a<i.length;){let l=i[a];if(l===0){a+=4;continue}else if(l===5){a+=2;continue}else if(typeof l=="number")break;if(!e&&Object.hasOwn(r,l)){let c=r[l];for(let u of c)if(u===n){s??=[],s.push(l,i[a+1]);break}}else if(e&&Object.hasOwn(o,l)){let c=o[l];for(let u=0;u<c.length;u+=2)if(c[u]===n){s??=[],s.push(c[u+1],i[a+1]);break}}a+=2}t.initialInputs??=[],t.initialInputs.push(s)}function $T(t,n,e,i,r){t.data[i]=r;let o=r.factory||(r.factory=Tr(r.type,!0)),s=new $r(o,fn(r),ft,null);t.blueprint[i]=s,e[i]=s,GT(t,n,i,sS(t,e,r.hostVars,bn),r)}function GT(t,n,e,i,r){let o=r.hostBindings;if(o){let s=t.hostBindingOpCodes;s===null&&(s=t.hostBindingOpCodes=[]);let a=~n.index;WT(s)!=a&&s.push(a),s.push(e,i,o)}}function WT(t){let n=t.length;for(;n>0;){let e=t[--n];if(typeof e=="number"&&e<0)return e}return 0}function qT(t,n,e){if(e){if(n.exportAs)for(let i=0;i<n.exportAs.length;i++)e[n.exportAs[i]]=t;fn(n)&&(e[""]=t)}}function YT(t,n,e){t.flags|=1,t.directiveStart=n,t.directiveEnd=n+e,t.providerIndexes=n}function OS(t,n,e,i,r,o,s,a){let l=n[Z],c=l.consts,u=jt(c,s),f=Xo(l,t,e,i,u);return o&&AS(l,n,f,jt(c,a),r),f.mergedAttrs=zo(f.mergedAttrs,f.attrs),f.attrs!==null&&Dd(f,f.attrs,!1),f.mergedAttrs!==null&&Dd(f,f.mergedAttrs,!0),l.queries!==null&&l.queries.elementStart(l,f),f}function PS(t,n){my(t,n),Ph(n)&&t.queries.elementEnd(n)}function ZT(t,n,e,i,r,o){let s=n.consts,a=jt(s,r),l=Xo(n,t,e,i,a);if(l.mergedAttrs=zo(l.mergedAttrs,l.attrs),o!=null){let c=jt(s,o);l.localNames=[];for(let u=0;u<c.length;u+=2)l.localNames.push(c[u],-1)}return l.attrs!==null&&Dd(l,l.attrs,!1),l.mergedAttrs!==null&&Dd(l,l.mergedAttrs,!0),n.queries!==null&&n.queries.elementStart(n,l),l}var FS=typeof ShadowRoot<"u",XT=typeof Document<"u";function KT(t){return Object.keys(t).map(n=>{let[e,i,r]=t[n],o={propName:e,templateName:n,isSignal:(i&Vd.SignalBased)!==0};return r&&(o.transform=r),o})}function QT(t){return Object.keys(t).map(n=>({propName:t[n],templateName:n}))}function JT(t,n,e){let i=n instanceof Ie?n:n?.injector;return i&&t.getStandaloneInjector!==null&&(i=t.getStandaloneInjector(i)||i),i?new Cd(e,i):e}function eN(t){let n=t.get(Ke,null);if(n===null)throw new x(407,!1);let e=t.get(MS,null),i=t.get(Rn,null),r=t.get(Un,null,{optional:!0});return{rendererFactory:n,sanitizer:e,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function tN(t,n,e){let i=LS(t);return Wy(n,i,i==="svg"?Fh:i==="math"?Xb:e)}function nN(t){if((t&&"localName"in t&&typeof t.localName=="string"?t.localName:t?.tagName)?.toLowerCase()==="script")throw new x(905,!1)}function LS(t){return(t.selectors[0][0]||"div").toLowerCase()}var Wo=class{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=KT(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=QT(this.componentDef.outputs),this.cachedOutputs}constructor(n,e){this.componentDef=n,this.ngModule=e,this.componentType=n.type,this.selector=wM(n.selectors),this.ngContentSelectors=n.ngContentSelectors??[],this.isBoundToModule=!!e}create(n,e,i,r,o,s,a){Ee(_e.DynamicComponentStart);let l=K(null);try{let c=this.componentDef,u=JT(c,r||this.ngModule,n),f=eN(u),h=f.tracingService;return h&&h.componentCreate?h.componentCreate(TS(c),()=>this.createComponentRef(f,u,e,i,o,s,a)):this.createComponentRef(f,u,e,i,o,s,a)}finally{K(l)}}createComponentRef(n,e,i,r,o,s,a){let l=this.componentDef,c=iN(r,l,s,o),u=n.rendererFactory.createRenderer(null,l),f=r?eT(u,r,l.encapsulation,e):tN(l,u,a??null);nN(f);let h=e.get(Wr,null),p=rN(f,()=>e.get(L,null)??Ay());h&&h.addHost(p);let S=s?.some(X_)||o?.some(F=>typeof F!="function"&&F.bindings.some(X_)),C=Cp(null,c,null,512|oS(l),null,null,n,u,e,null,Ly(f,e,!0));h&&FS&&p instanceof ShadowRoot&&Kc(C,()=>{h.removeHost(p)}),C[Ye]=f,ed(C);let $=null;try{let F=OS(Ye,C,2,"#host",()=>c.directiveRegistry,!0,0);Zy(u,f,F),$o(f,C),Ep(c,C,F),By(c,F,C),PS(c,F),i!==void 0&&sN(F,this.ngContentSelectors,i),$=qt(F.index,C),C[qe]=$[qe],xp(c,C,null)}catch(F){throw $!==null&&Sm($),Sm(C),F}finally{Ee(_e.DynamicComponentEnd),td()}return new Ed(this.componentType,C,!!S)}};function iN(t,n,e,i){let r=t?["ng-version","22.1.6"]:xM(n.selectors[0]),o=null,s=null,a=0;if(e)for(let u of e)a+=u[Fm].requiredVars,u.create&&(u.targetIdx=0,(o??=[]).push(u)),u.update&&(u.targetIdx=0,(s??=[]).push(u));if(i)for(let u=0;u<i.length;u++){let f=i[u];if(typeof f!="function")for(let h of f.bindings){a+=h[Fm].requiredVars;let p=u+1;h.create&&(h.targetIdx=p,(o??=[]).push(h)),h.update&&(h.targetIdx=p,(s??=[]).push(h))}}let l=[n];if(i)for(let u of i){let f=typeof u=="function"?u:u.type,h=jc(f);l.push(h)}return Sp(0,null,oN(o,s),1,a,l,null,null,null,[r],null)}function rN(t,n){let e=t.getRootNode?.();return XT&&e instanceof Document?e.head:e&&FS&&e instanceof ShadowRoot?e:n().head}function oN(t,n){return!t&&!n?null:e=>{if(e&1&&t)for(let i of t)i.create();if(e&2&&n)for(let i of n)i.update()}}function X_(t){let n=t[Fm].kind;return n==="input"||n==="twoWay"}var Ed=class extends IS{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(n,e,i){super(),this._rootLView=e,this._hasInputBindings=i,this._tNode=Yc(e[Z],Ye),this.location=Zo(this._tNode,e),this.instance=qt(this._tNode.index,e)[qe],this.hostView=this.changeDetectorRef=new ji(e,void 0),this.componentType=n}setInput(n,e){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(n)&&Object.is(this.previousInputValues.get(n),e))return;let r=this._rootLView,o=wp(i,r[Z],r,n,e);this.previousInputValues.set(n,e);let s=qt(i.index,r);Ip(s,1)}get injector(){return new Vi(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(n){this.hostView.onDestroy(n)}};function sN(t,n,e){let i=t.projection=[];for(let r=0;r<n.length;r++){let o=e[r];i.push(o!=null&&o.length?Array.from(o):null)}}var bt=(()=>{class t{static __NG_ELEMENT_ID__=aN}return t})();function aN(){let t=vt();return VS(t,le())}var Lm=class t extends bt{_lContainer;_hostTNode;_hostLView;constructor(n,e,i){super(),this._lContainer=n,this._hostTNode=e,this._hostLView=i}get element(){return Zo(this._hostTNode,this._hostLView)}get injector(){return new Vi(this._hostTNode,this._hostLView)}get parentInjector(){let n=tp(this._hostTNode,this._hostLView);if(vy(n)){let e=_d(n,this._hostLView),i=bd(n),r=e[Z].data[i+8];return new Vi(r,e)}else return new Vi(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(n){let e=K_(this._lContainer);return e!==null&&e[n]||null}get length(){return this._lContainer.length-je}createEmbeddedView(n,e,i){let r,o;typeof i=="number"?r=i:i!=null&&(r=i.index,o=i.injector);let s=Sd(this._lContainer,n.ssrId),a=n.createEmbeddedViewImpl(e||{},o,s);return this.insertImpl(a,r,Go(this._hostTNode,s)),a}createComponent(n,e,i,r,o,s,a){let l,c=e||{};l=c.index,i=c.injector,r=c.projectableNodes,o=c.environmentInjector||c.ngModuleRef,s=c.directives,a=c.bindings;let u=new Wo(ki(n)),f=i||this.parentInjector;if(!o&&u.ngModule==null){let F=this.parentInjector.get(Ie,null);F&&(o=F)}let h=ki(u.componentType??{}),p=Sd(this._lContainer,h?.id??null),S=p?.firstChild??null,C=u.create(f,r,S,o,s,a,this._getHostElementNamespace());return this.insertImpl(C.hostView,l,Go(this._hostTNode,p)),C}_getHostElementNamespace(){if(this._hostTNode.type&2){let n=this._hostTNode.parent??this._hostLView[pt];return n!==null&&n.type&2&&typeof n.value=="string"&&n.value.toLowerCase()==="foreignobject"?null:n?.namespace??null}return this._hostTNode.namespace}insert(n,e){return this.insertImpl(n,e,!0)}insertImpl(n,e,i){let r=n._lView;if(Jb(r)){let a=this.indexOf(n);if(a!==-1)this.detach(a);else{let l=r[rt],c=new t(l,l[pt],l[rt]);c.detach(c.indexOf(n))}}let o=this._adjustIndex(e),s=this._lContainer;return Na(s,r,o,i),n.attachToViewContainerRef(),Ih(dm(s),o,n),n}move(n,e){return this.insert(n,e)}indexOf(n){let e=K_(this._lContainer);return e!==null?e.indexOf(n):-1}remove(n){let e=this._adjustIndex(n,-1),i=Da(this._lContainer,e);i&&(la(dm(this._lContainer),e),Fd(i[Z],i))}detach(n){let e=this._adjustIndex(n,-1),i=Da(this._lContainer,e);return i&&la(dm(this._lContainer),e)!=null?new ji(i):null}_adjustIndex(n,e=0){return n??this.length+e}};function K_(t){return t[ua]}function dm(t){return t[ua]||(t[ua]=[])}function VS(t,n){let e,i=n[t.index];return Wt(i)?e=i:(e=CS(i,n,null,t),n[t.index]=e,Dp(n,e)),cN(e,n,t,i),new Lm(e,t,n)}function lN(t,n){let e=t[Fe],i=e.createComment(""),r=hn(n,t),o=e.parentNode(r);return zr(e,o,i,e.nextSibling(r),!1),i}var cN=fN,dN=()=>!1;function uN(t,n,e){return dN(t,n,e)}function fN(t,n,e,i){if(t[Lr])return;let r;e.type&8?r=gt(i):r=lN(n,e),t[Lr]=r}var Vm=class t{queryList;matches=null;constructor(n){this.queryList=n}clone(){return new t(this.queryList)}setDirty(){this.queryList.setDirty()}},Bm=class t{queries;constructor(n=[]){this.queries=n}createEmbeddedView(n){let e=n.queries;if(e!==null){let i=n.contentQueries!==null?n.contentQueries[0]:e.length,r=[];for(let o=0;o<i;o++){let s=e.getByIndex(o),a=this.queries[s.indexInDeclarationView];r.push(a.clone())}return new t(r)}return null}insertView(n){this.dirtyQueriesWithMatches(n)}detachView(n){this.dirtyQueriesWithMatches(n)}finishViewCreation(n){this.dirtyQueriesWithMatches(n)}dirtyQueriesWithMatches(n){for(let e=0;e<this.queries.length;e++)Tp(n,e).matches!==null&&this.queries[e].setDirty()}},wd=class{flags;read;predicate;constructor(n,e,i=null){this.flags=e,this.read=i,typeof n=="string"?this.predicate=vN(n):this.predicate=n}},jm=class t{queries;constructor(n=[]){this.queries=n}elementStart(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(n,e)}elementEnd(n){for(let e=0;e<this.queries.length;e++)this.queries[e].elementEnd(n)}embeddedTView(n){let e=null;for(let i=0;i<this.length;i++){let r=e!==null?e.length:0,o=this.getByIndex(i).embeddedTView(n,r);o&&(o.indexInDeclarationView=i,e!==null?e.push(o):e=[o])}return e!==null?new t(e):null}template(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].template(n,e)}getByIndex(n){return this.queries[n]}get length(){return this.queries.length}track(n){this.queries.push(n)}},Hm=class t{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(n,e=-1){this.metadata=n,this._declarationNodeIndex=e}elementStart(n,e){this.isApplyingToNode(e)&&this.matchTNode(n,e)}elementEnd(n){this._declarationNodeIndex===n.index&&(this._appliesToNextNode=!1)}template(n,e){this.elementStart(n,e)}embeddedTView(n,e){return this.isApplyingToNode(n)?(this.crossesNgTemplate=!0,this.addMatch(-n.index,e),new t(this.metadata)):null}isApplyingToNode(n){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let e=this._declarationNodeIndex,i=n.parent;for(;i!==null&&i.type&8&&i.index!==e;)i=i.parent;return e===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(n,e){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let o=i[r];this.matchTNodeWithReadOption(n,e,hN(e,o)),this.matchTNodeWithReadOption(n,e,md(e,n,o,!1,!1))}else i===Ct?e.type&4&&this.matchTNodeWithReadOption(n,e,-1):this.matchTNodeWithReadOption(n,e,md(e,n,i,!1,!1))}matchTNodeWithReadOption(n,e,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===O||r===bt||r===Ct&&e.type&4)this.addMatch(e.index,-2);else{let o=md(e,n,r,!1,!1);o!==null&&this.addMatch(e.index,o)}else this.addMatch(e.index,i)}}addMatch(n,e){this.matches===null?this.matches=[n,e]:this.matches.push(n,e)}};function hN(t,n){let e=t.localNames;if(e!==null){for(let i=0;i<e.length;i+=2)if(e[i]===n)return e[i+1]}return null}function mN(t,n){return t.type&11?Zo(t,n):t.type&4?jd(t,n):null}function pN(t,n,e,i){return e===-1?mN(n,t):e===-2?gN(t,n,i):ya(t,t[Z],e,n)}function gN(t,n,e){if(e===O)return Zo(n,t);if(e===Ct)return jd(n,t);if(e===bt)return VS(n,t)}function BS(t,n,e,i){let r=n[Fn].queries[i];if(r.matches===null){let o=t.data,s=e.matches,a=[];for(let l=0;s!==null&&l<s.length;l+=2){let c=s[l];if(c<0)a.push(null);else{let u=o[c];a.push(pN(n,u,s[l+1],e.metadata.read))}}r.matches=a}return r.matches}function Um(t,n,e,i){let r=t.queries.getByIndex(e),o=r.matches;if(o!==null){let s=BS(t,n,r,e);for(let a=0;a<o.length;a+=2){let l=o[a];if(l>0)i.push(s[a/2]);else{let c=o[a+1],u=n[-l];for(let f=je;f<u.length;f++){let h=u[f];h[Oi]===h[rt]&&Um(h[Z],h,c,i)}if(u[Vr]!==null){let f=u[Vr];for(let h=0;h<f.length;h++){let p=f[h];Um(p[Z],p,c,i)}}}}}return i}function Mp(t,n){return t[Fn].queries[n].queryList}function jS(t,n,e){let i=new ai((e&4)===4);return n_(t,n,i,i.destroy),(n[Fn]??=new Bm).queries.push(new Vm(i))-1}function HS(t,n,e){let i=He();return i.firstCreatePass&&(zS(i,new wd(t,n,e),-1),(n&2)===2&&(i.staticViewQueries=!0)),jS(i,le(),n)}function US(t,n,e,i){let r=He();if(r.firstCreatePass){let o=vt();zS(r,new wd(n,e,i),o.index),bN(r,t),(e&2)===2&&(r.staticContentQueries=!0)}return jS(r,le(),e)}function vN(t){return t.split(",").map(n=>n.trim())}function zS(t,n,e){t.queries===null&&(t.queries=new jm),t.queries.track(new Hm(n,e))}function bN(t,n){let e=t.contentQueries||(t.contentQueries=[]),i=e.length?e[e.length-1]:-1;n!==i&&e.push(t.queries.length-1,n)}function Tp(t,n){return t.queries.getByIndex(n)}function $S(t,n){let e=t[Z],i=Tp(e,n);return i.crossesNgTemplate?Um(e,t,n,[]):BS(e,t,i,n)}function GS(t,n,e){let i,r=Hs(()=>{i._dirtyCounter();let o=_N(i,t);if(n&&o===void 0)throw new x(-951,!1);return o});return i=r[et],i._dirtyCounter=j(0),i._flatValue=void 0,r}function Np(t){return GS(!0,!1,t)}function kp(t){return GS(!0,!0,t)}function WS(t,n){let e=t[et];e._lView=le(),e._queryIndex=n,e._queryList=Mp(e._lView,n),e._queryList.onDirty(()=>e._dirtyCounter.update(i=>i+1))}function _N(t,n){let e=t._lView,i=t._queryIndex;if(e===void 0||i===void 0||e[J]&4)return n?void 0:St;let r=Mp(e,i),o=$S(e,i);return r.reset(o,xy),n?r.first:r._changesDetected||t._flatValue===void 0?t._flatValue=r.toArray():t._flatValue}function di(t){return!!t&&typeof t.then=="function"}function Ap(t){return!!t&&typeof t.subscribe=="function"}var Hn=class{},Ud=class{};var xd=class extends Hn{ngModuleType;_parent;_bootstrapComponents=[];_r3Injector;instance;destroyCbs=[];constructor(n,e,i,r=!0){super(),this.ngModuleType=n,this._parent=e;let o=Ob(n);this._bootstrapComponents=mM(o.bootstrap),this._r3Injector=tm(n,e,[{provide:Hn,useValue:this},...i],ra(n),new Set(["environment"])),r&&this.resolveInjectorInitializers()}resolveInjectorInitializers(){this._r3Injector.resolveInjectorInitializers(),this.instance=this._r3Injector.get(this.ngModuleType)}get injector(){return this._r3Injector}destroy(){let n=this._r3Injector;!n.destroyed&&n.destroy(),this.destroyCbs.forEach(e=>e()),this.destroyCbs=null}onDestroy(n){this.destroyCbs.push(n)}},Id=class extends Ud{moduleType;constructor(n){super(),this.moduleType=n}create(n){return new xd(this.moduleType,n,[])}};var Ea=class extends Hn{injector;instance=null;constructor(n){super();let e=new kr([...n.providers,{provide:Hn,useValue:this}],n.parent||Po(),n.debugName,new Set(["environment"]));this.injector=e,n.runEnvironmentInitializers&&e.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(n){this.injector.onDestroy(n)}};function ka(t,n,e=null){return new Ea({providers:t,parent:n,debugName:e,runEnvironmentInitializers:!0}).injector}var yN=(()=>{class t{_injector;cachedInjectors=new Map;constructor(e){this._injector=e}getOrCreateStandaloneInjector(e){if(!e.standalone)return null;if(!this.cachedInjectors.has(e)){let i=Th(!1,e.type),r=i.length>0?ka([i],this._injector,""):null;this.cachedInjectors.set(e,r)}return this.cachedInjectors.get(e)}ngOnDestroy(){try{for(let e of this.cachedInjectors.values())e!==null&&e.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=G({token:t,providedIn:"environment",factory:()=>new t(A(Ie))})}return t})();function T(t){return xa(()=>{let n=qS(t),e=k(v({},n),{decls:t.decls,vars:t.vars,template:t.template,consts:t.consts||null,ngContentSelectors:t.ngContentSelectors,onPush:t.changeDetection!==ip.Eager,directiveDefs:null,pipeDefs:null,dependencies:n.standalone&&t.dependencies||null,getStandaloneInjector:n.standalone?r=>r.get(yN).getOrCreateStandaloneInjector(e):null,getExternalStyles:null,signals:t.signals??!1,data:t.data||{},encapsulation:t.encapsulation||gn.Emulated,styles:t.styles||St,_:null,schemas:t.schemas||null,tView:null,id:""});n.standalone&&Ui("NgStandalone"),YS(e);let i=t.dependencies;return e.directiveDefs=Q_(i,SN),e.pipeDefs=Q_(i,Pb),e.id=EN(e),e})}function SN(t){return ki(t)||jc(t)}function X(t){return xa(()=>({type:t.type,bootstrap:t.bootstrap||St,declarations:t.declarations||St,imports:t.imports||St,exports:t.exports||St,transitiveCompileScopes:null,schemas:t.schemas||null,id:t.id||null}))}function CN(t,n){if(t==null)return Ai;let e={};for(let i in t)if(Object.hasOwn(t,i)){let r=t[i],o,s,a,l;Array.isArray(r)?(a=r[0],o=r[1],s=r[2]??o,l=r[3]||null):(o=r,s=r,a=Vd.None,l=null),e[o]=[i,a,l],n[o]=s}return e}function DN(t){if(t==null)return Ai;let n={};for(let e in t)Object.hasOwn(t,e)&&(n[t[e]]=e);return n}function H(t){return xa(()=>{let n=qS(t);return YS(n),n})}function qS(t){let n={};return{type:t.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:t.hostBindings||null,hostVars:t.hostVars||0,hostAttrs:t.hostAttrs||null,contentQueries:t.contentQueries||null,declaredInputs:n,inputConfig:t.inputs||Ai,exportAs:t.exportAs||null,standalone:t.standalone??!0,signals:t.signals===!0,selectors:t.selectors||St,viewQuery:t.viewQuery||null,features:t.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,signalFormsInputPresence:null,inputs:CN(t.inputs,n),outputs:DN(t.outputs),debugInfo:null}}function YS(t){t.features?.forEach(n=>n(t))}function Q_(t,n){return t?()=>{let e=typeof t=="function"?t():t,i=[];for(let r of e){let o=n(r);o!==null&&i.push(o)}return i}:null}function EN(t){let n=0,e=typeof t.consts=="function"?"":t.consts,i=[t.selectors,t.ngContentSelectors,t.hostVars,t.hostAttrs,e,t.vars,t.decls,t.encapsulation,t.standalone,t.signals,t.exportAs,JSON.stringify(t.inputs),JSON.stringify(t.outputs),Object.getOwnPropertyNames(t.type.prototype),!!t.contentQueries,!!t.viewQuery];for(let o of i.join("|"))n=Math.imul(31,n)+o.charCodeAt(0)<<0;return n+=2147483648,"c"+n}var ZS=new b("");var Rp=(()=>{class t{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((e,i)=>{this.resolve=e,this.reject=i});appInits=d(ZS,{optional:!0})??[];injector=d(R);constructor(){}runInitializers(){if(this.initialized)return;let e=[];for(let r of this.appInits){let o=ot(this.injector,r);if(di(o))e.push(o);else if(Ap(o)){let s=new Promise((a,l)=>{o.subscribe({complete:a,error:l})});e.push(s)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(e).then(()=>{i()}).catch(r=>{this.reject(r)}),e.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();function Op(t){let n=e=>{let i=Array.isArray(t);e.hostDirectives===null?(e.resolveHostDirectives=wN,e.hostDirectives=i?t.map(zm):[t]):i?e.hostDirectives.unshift(...t.map(zm)):e.hostDirectives.unshift(t)};return n.ngInherit=!0,n}function wN(t){let n=[],e=!1,i=null,r=null;for(let o=0;o<t.length;o++){let s=t[o];if(s.hostDirectives!==null){let a=n.length;i??=new Map,r??=new Map,XS(s,n,i,t),r.set(s,[a,n.length-1])}o===0&&fn(s)&&(e=!0,n.push(s))}for(let o=e?1:0;o<t.length;o++)n.push(t[o]);return i!==null&&i.forEach((o,s)=>{xN(s.declaredInputs,o.inputs)}),[n,i,r]}function XS(t,n,e,i){if(t.hostDirectives!==null)for(let r of t.hostDirectives)if(typeof r=="function"){let o=r();for(let s of o)J_(zm(s),n,e,i)}else J_(r,n,e,i)}function J_(t,n,e,i){let r=jc(t.directive);if(XS(r,n,e,i),e.has(r)){let o=e.get(r);ey(o,t.inputs,"input"),ey(o,t.outputs,"output")}else i.includes(r)||(e.set(r,t),n.push(r))}function ey(t,n,e){let i=e==="input"?t.inputs:t.outputs;Object.keys(n).forEach(r=>{let o=n[r];(!Object.hasOwn(i,r)||i[r]===o)&&(i[r]=o)})}function zm(t){return typeof t=="function"?{directive:it(t),inputs:{},outputs:{}}:{directive:it(t.directive),inputs:ty(t.inputs),outputs:ty(t.outputs)}}function ty(t){let n={};if(t!==void 0&&t.length>0)for(let e=0;e<t.length;e+=2)n[t[e]]=t[e+1];return n}function xN(t,n){for(let e in n)if(Object.hasOwn(n,e)){let i=n[e],r=t[e];t[i]=r}}function IN(t){return Object.getPrototypeOf(t.prototype).constructor}function Me(t){let n=IN(t.type),e=!0,i=[t];for(;n&&n!==Function.prototype&&n!==Object.prototype;){let r,o=Object.hasOwn(n,sa)?n[sa]:void 0,s=Object.hasOwn(n,aa)?n[aa]:void 0;if(fn(t))r=o??s;else{if(o)throw new x(903,!1);r=s}if(r){if(e){i.push(r);let l=t;l.inputs=um(t.inputs),l.declaredInputs=um(t.declaredInputs),l.outputs=um(t.outputs);let c=r.hostBindings;c&&AN(t,c);let u=r.viewQuery,f=r.contentQueries;if(u&&NN(t,u),f&&kN(t,f),MN(t,r),Rb(t.outputs,r.outputs),fn(r)&&r.data.animation){let h=t.data;h.animation=(h.animation||[]).concat(r.data.animation)}}let a=r.features;if(a)for(let l=0;l<a.length;l++){let c=a[l];c&&c.ngInherit&&c(t),c===Me&&(e=!1)}}n=Object.getPrototypeOf(n)}TN(i)}function MN(t,n){for(let e in n.inputs){if(!Object.hasOwn(n.inputs,e)||Object.hasOwn(t.inputs,e))continue;let i=n.inputs[e];i!==void 0&&(t.inputs[e]=i,t.declaredInputs[e]=n.declaredInputs[e])}}function TN(t){let n=0,e=null;for(let i=t.length-1;i>=0;i--){let r=t[i];r.hostVars=n+=r.hostVars,r.hostAttrs=zo(r.hostAttrs,e=zo(e,r.hostAttrs))}}function um(t){return t===Ai?{}:t===St?[]:t}function NN(t,n){let e=t.viewQuery;e?t.viewQuery=(i,r)=>{n(i,r),e(i,r)}:t.viewQuery=n}function kN(t,n){let e=t.contentQueries;e?t.contentQueries=(i,r,o)=>{n(i,r,o),e(i,r,o)}:t.contentQueries=n}function AN(t,n){let e=t.hostBindings;e?t.hostBindings=(i,r)=>{n(i,r),e(i,r)}:t.hostBindings=n}function KS(t,n,e,i,r,o,s,a){if(e.firstCreatePass){t.mergedAttrs=zo(t.mergedAttrs,t.attrs);let u=t.tView=Sp(2,t,r,o,s,e.directiveRegistry,e.pipeRegistry,null,e.schemas,e.consts,null);e.queries!==null&&(e.queries.template(e,t),u.queries=e.queries.embeddedTView(t))}a&&(t.flags|=a),Bo(t,!1);let l=ON(e,n,t,i);rd()&&_p(e,n,l,t),$o(l,n);let c=CS(l,n,l,t);n[i+Ye]=c,Dp(n,c),uN(c,t,n)}function RN(t,n,e,i,r,o,s,a,l,c,u){let f=e+Ye,h;return n.firstCreatePass?(h=Xo(n,f,4,s||null,a||null),zh()&&AS(n,t,h,jt(n.consts,c),dS),my(n,h)):h=n.data[f],KS(h,t,n,e,i,r,o,l),fa(h)&&Ep(n,t,h),c!=null&&Bd(t,h,u),h}function qo(t,n,e,i,r,o,s,a,l,c,u){let f=e+Ye,h;if(n.firstCreatePass){if(h=Xo(n,f,4,s||null,a||null),c!=null){let p=jt(n.consts,c);h.localNames=[];for(let S=0;S<p.length;S+=2)h.localNames.push(p[S],-1)}}else h=n.data[f];return KS(h,t,n,e,i,r,o,l),c!=null&&Bd(t,h,u),h}function Dt(t,n,e,i,r,o,s,a){let l=le(),c=He(),u=jt(c.consts,o);return RN(l,c,t,n,e,i,r,u,void 0,s,a),Dt}function zd(t,n,e,i,r,o,s,a){let l=le(),c=He(),u=jt(c.consts,o);return qo(l,c,t,n,e,i,r,u,void 0,s,a),zd}var ON=PN;function PN(t,n,e,i){return od(!0),n[Fe].createComment("")}var $d=(()=>{class t{log(e){console.log(e)}warn(e){console.warn(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=G({token:t,factory:t.\u0275fac,providedIn:"platform"})}return t})();var Pp=new b("");var Aa=new b("");function QS(){Gf(()=>{let t="";throw new x(600,t)})}var FN=10;var Ft=(()=>{class t{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=d(Yt);afterRenderManager=d(Od);zonelessEnabled=d(ga);rootEffectScheduler=d(ad);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new E;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=d(oi);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(oe(e=>!e))}constructor(){d(Un,{optional:!0})}whenStable(){let e;return new Promise(i=>{e=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{e.unsubscribe()})}_injector=d(Ie);_rendererFactory=null;get injector(){return this._injector}bootstrap(e,i){return this.bootstrapImpl(e,i)}bootstrapImpl(e,i,r=R.NULL){return this._injector.get(N).run(()=>{if(Ee(_e.BootstrapComponentStart),!this._injector.get(Rp).done){let F="";throw new x(405,F)}let a=ki(e),l=this._injector.get(Hn),c=new Wo(a,l);this.componentTypes.push(e);let{hostElement:u,directives:f,bindings:h}=LN(i),p=u||c.selector,S=c.create(r,[],p,l.injector,f,h),C=S.location.nativeElement,$=S.injector.get(Pp,null);return $?.registerApplication(C),S.onDestroy(()=>{this.detachView(S.hostView),_a(this.components,S),$?.unregisterApplication(C)}),this._loadComponent(S),Ee(_e.BootstrapComponentEnd,S),S})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){Ee(_e.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(Rd.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw Ee(_e.ChangeDetectionEnd),new x(101,!1);let e=K(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,K(e),this.afterTick.next(),Ee(_e.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(Ke,null,{optional:!0}));let e=0;for(;this.dirtyFlags!==0&&e++<FN;){Ee(_e.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{Ee(_e.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let e=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!ha(r))continue;let o=i&&!this.zonelessEnabled?0:1;bS(r,o),e=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}e||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:e})=>ha(e))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(e){let i=e;this._views.push(i),i.attachToAppRef(this)}detachView(e){let i=e;_a(this._views,i),i.detachFromAppRef()}_loadComponent(e){this.attachView(e.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(e),this._injector.get(Aa,[]).forEach(r=>r(e))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(e=>e()),this._views.slice().forEach(e=>e.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(e){return this._destroyListeners.push(e),()=>_a(this._destroyListeners,e)}destroy(){if(this._destroyed)throw new x(406,!1);let e=this._injector;e.destroy&&!e.destroyed&&e.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();function LN(t){return t===void 0||typeof t=="string"||t instanceof Element?{hostElement:t}:t}function _a(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function re(t,n,e,i){let r=le(),o=Hr();if(zi(r,o,n)){let s=He(),a=nd();cT(a,r,t,n,e,i)}return re}var $m=class{destroy(n){}updateValue(n,e){}swap(n,e){let i=Math.min(n,e),r=Math.max(n,e),o=this.detach(r);if(r-i>1){let s=this.detach(i);this.attach(i,o),this.attach(r,s)}else this.attach(i,o)}move(n,e){this.attach(e,this.detach(n))}};function fm(t,n,e,i,r){return t===e&&Object.is(n,i)?1:Object.is(r(t,n),r(e,i))?-1:0}function VN(t,n,e,i){let r,o,s=0,a=t.length-1,l=void 0;if(Array.isArray(n)){K(i);let c=n.length-1;for(K(null);s<=a&&s<=c;){let u=t.at(s),f=n[s],h=fm(s,u,s,f,e);if(h!==0){h<0&&t.updateValue(s,f),s++;continue}let p=t.at(a),S=n[c],C=fm(a,p,c,S,e);if(C!==0){C<0&&t.updateValue(a,S),a--,c--;continue}let $=e(s,u),F=e(a,p),we=e(s,f);if(Object.is(we,F)){let Je=e(c,S);Object.is(Je,$)?(t.swap(s,a),t.updateValue(a,S),c--,a--):t.move(a,s),t.updateValue(s,f),s++;continue}if(r??=new Md,o??=iy(t,s,a,e),Gm(t,r,s,we))t.updateValue(s,f),s++,a++;else if(o.has(we))r.set($,t.detach(s)),a--;else{let Je=t.create(s,n[s]);t.attach(s,Je),s++,a++}}for(;s<=c;)ny(t,r,e,s,n[s]),s++}else if(n!=null){K(i);let c=n[Symbol.iterator]();K(null);let u=c.next();for(;!u.done&&s<=a;){let f=t.at(s),h=u.value,p=fm(s,f,s,h,e);if(p!==0)p<0&&t.updateValue(s,h),s++,u=c.next();else{r??=new Md,o??=iy(t,s,a,e);let S=e(s,h);if(Gm(t,r,s,S))t.updateValue(s,h),s++,a++,u=c.next();else if(!o.has(S))t.attach(s,t.create(s,h)),s++,a++,u=c.next();else{let C=e(s,f);r.set(C,t.detach(s)),a--}}}for(;!u.done;)ny(t,r,e,t.length,u.value),u=c.next()}for(;s<=a;)t.destroy(t.detach(a--));r?.forEach(c=>{t.destroy(c)})}function Gm(t,n,e,i){return n!==void 0&&n.has(i)?(t.attach(e,n.get(i)),n.delete(i),!0):!1}function ny(t,n,e,i,r){if(Gm(t,n,i,e(i,r)))t.updateValue(i,r);else{let o=t.create(i,r);t.attach(i,o)}}function iy(t,n,e,i){let r=new Set;for(let o=n;o<=e;o++)r.add(i(o,t.at(o)));return r}var Md=class{kvMap=new Map;_vMap=void 0;has(n){return this.kvMap.has(n)}delete(n){if(!this.has(n))return!1;let e=this.kvMap.get(n);return this._vMap!==void 0&&this._vMap.has(e)?(this.kvMap.set(n,this._vMap.get(e)),this._vMap.delete(e)):this.kvMap.delete(n),!0}get(n){return this.kvMap.get(n)}set(n,e){if(this.kvMap.has(n)){let i=this.kvMap.get(n);this._vMap===void 0&&(this._vMap=new Map);let r=this._vMap;for(;r.has(i);)i=r.get(i);r.set(i,e)}else this.kvMap.set(n,e)}forEach(n){for(let[e,i]of this.kvMap)if(n(i,e),this._vMap!==void 0){let r=this._vMap;for(;r.has(i);)i=r.get(i),n(i,e)}}};function V(t,n,e,i,r,o,s,a){Ui("NgControlFlow");let l=le(),c=He(),u=jt(c.consts,o);return qo(l,c,t,n,e,i,r,u,256,s,a),Fp}function Fp(t,n,e,i,r,o,s,a){Ui("NgControlFlow");let l=le(),c=He(),u=jt(c.consts,o);return qo(l,c,t,n,e,i,r,u,512,s,a),Fp}function B(t,n){Ui("NgControlFlow");let e=le(),i=Hr(),r=e[i]!==bn?e[i]:-1,o=r!==-1?Td(e,Ye+r):void 0,s=0;if(zi(e,i,t)){let a=K(null);try{if(o!==void 0&&ES(o,s),t!==-1){let l=Ye+t,c=Td(e,l),u=Zm(e[Z],l),f=xS(c,u,e),h=Ta(e,u,n,{dehydratedView:f});Na(c,h,s,Go(u,f))}}finally{K(a)}}else if(o!==void 0){let a=DS(o,s);a!==void 0&&(a[qe]=n)}}var Wm=class{lContainer;$implicit;$index;constructor(n,e,i){this.lContainer=n,this.$implicit=e,this.$index=i}get $count(){return this.lContainer.length-je}};function Lp(t){return t}function Gd(t,n){return n}var qm=class{hasEmptyBlock;trackByFn;liveCollection;constructor(n,e,i){this.hasEmptyBlock=n,this.trackByFn=e,this.liveCollection=i}};function Xt(t,n,e,i,r,o,s,a,l,c,u,f,h){Ui("NgControlFlow");let p=le(),S=He(),C=l!==void 0,$=le(),F=a?s.bind($[kt][qe]):s,we=new qm(C,F);$[Ye+t]=we,qo(p,S,t+1,n,e,i,r,jt(S.consts,o),256),C&&qo(p,S,t+2,l,c,u,f,jt(S.consts,h),512)}var Ym=class extends $m{lContainer;hostLView;templateTNode;operationsCounter=void 0;needsIndexUpdate=!1;constructor(n,e,i){super(),this.lContainer=n,this.hostLView=e,this.templateTNode=i}get length(){return this.lContainer.length-je}at(n){return this.getLView(n)[qe].$implicit}attach(n,e){let i=e[Or];this.needsIndexUpdate||=n!==this.length,Na(this.lContainer,e,n,Go(this.templateTNode,i)),BN(this.lContainer,n)}detach(n){return this.needsIndexUpdate||=n!==this.length-1,jN(this.lContainer,n),HN(this.lContainer,n)}create(n,e){let i=Sd(this.lContainer,this.templateTNode.tView.ssrId);return Ta(this.hostLView,this.templateTNode,new Wm(this.lContainer,e,n),{dehydratedView:i})}destroy(n){Fd(n[Z],n)}updateValue(n,e){this.getLView(n)[qe].$implicit=e}reset(){this.needsIndexUpdate=!1}updateIndexes(){if(this.needsIndexUpdate)for(let n=0;n<this.length;n++)this.getLView(n)[qe].$index=n}getLView(n){return UN(this.lContainer,n)}};function Kt(t){let n=K(null),e=ri();try{let i=le(),r=i[Z],o=i[e],s=e+1,a=Td(i,s);if(o.liveCollection===void 0){let c=Zm(r,s);o.liveCollection=new Ym(a,i,c)}else o.liveCollection.reset();let l=o.liveCollection;if(VN(l,t,o.trackByFn,n),l.updateIndexes(),o.hasEmptyBlock){let c=Hr(),u=l.length===0;if(zi(i,c,u)){let f=e+2,h=Td(i,f);if(u){let p=Zm(r,f),S=xS(h,p,i),C=Ta(i,p,void 0,{dehydratedView:S});Na(h,C,0,Go(p,S))}else r.firstUpdatePass&&RT(h),ES(h,0)}}}finally{K(n)}}function Td(t,n){return t[n]}function BN(t,n){if(t.length<=je)return;let e=je+n,i=t[e],r=i?i[Vn]:void 0;if(i&&r&&r.detachedLeaveAnimationFns&&r.detachedLeaveAnimationFns.length>0){let o=i[Pn];RM(o,r),Bi.delete(i[Ln]),r.detachedLeaveAnimationFns=void 0}}function jN(t,n){if(t.length<=je)return;let e=je+n,i=t[e],r=i?i[Vn]:void 0;r&&r.leave&&r.leave.size>0&&(r.detachedLeaveAnimationFns=[])}function HN(t,n){return Da(t,n)}function UN(t,n){return DS(t,n)}function Zm(t,n){return Yc(t,n)}function U(t,n,e){let i=le(),r=Hr();if(zi(i,r,n)){let o=He(),s=nd();rT(s,i,t,n,i[Fe],e)}return U}function Xm(t,n,e,i,r){wp(n,t,e,r?"class":"style",i)}function m(t,n,e,i){let r=le(),o=r[Z],s=t+Ye,a=o.firstCreatePass?OS(s,r,2,n,dS,zh(),e,i):o.data[s];if(ii(a)){let l=r[un].tracingService;if(l&&l.componentCreate){let c=o.data[a.directiveStart+a.componentOffset];return l.componentCreate(TS(c),()=>(ry(t,n,r,a,i),m))}}return ry(t,n,r,a,i),m}function ry(t,n,e,i,r){if(uS(i,e,t,n,JS),fa(i)){let o=e[Z];Ep(o,e,i),By(o,i,e)}r!=null&&Bd(e,i)}function g(){let t=He(),n=vt(),e=fS(n);return t.firstCreatePass&&PS(t,e),Gh(e)&&Wh(),Uh(),e.classesWithoutHost!=null&&EI(e)&&Xm(t,e,le(),e.classesWithoutHost,!0),e.stylesWithoutHost!=null&&wI(e)&&Xm(t,e,le(),e.stylesWithoutHost,!1),g}function ie(t,n,e,i){return m(t,n,e,i),g(),ie}function lt(t,n,e,i){let r=le(),o=r[Z],s=t+Ye,a=o.firstCreatePass?ZT(s,o,2,n,e,i):o.data[s];return uS(a,r,t,n,JS),i!=null&&Bd(r,a),lt}function _t(){let t=vt(),n=fS(t);return Gh(n)&&Wh(),Uh(),_t}function Qt(t,n,e,i){return lt(t,n,e,i),_t(),Qt}var JS=(t,n,e,i,r)=>(od(!0),Wy(n[Fe],i,em()));function Le(){return le()}function Et(t,n,e){let i=le(),r=Hr();if(zi(i,r,n)){let o=He(),s=nd();cS(s,i,t,n,i[Fe],e)}return Et}var Ra="en-US";var zN=Ra;function e0(t){typeof t=="string"&&(zN=t.toLowerCase().replace(/_/g,"-"))}function D(t,n,e){let i=le(),r=He(),o=vt();return $N(r,i,i[Fe],o,t,n,e),D}function Ko(t,n,e){let i=le(),r=He(),o=vt();return(o.type&3||e)&&NS(o,r,i,e,i[Fe],t,n,gd(o,i,n)),Ko}function $N(t,n,e,i,r,o,s){let a=!0,l=null;if((i.type&3||s)&&(l??=gd(i,n,o),NS(i,t,n,s,e,r,o,l)&&(a=!1)),a){let c=i.outputs?.[r],u=i.hostDirectiveOutputs?.[r];if(u&&u.length)for(let f=0;f<u.length;f+=2){let h=u[f],p=u[f+1];l??=gd(i,n,o),W_(i,n,h,p,r,l)}if(c&&c.length)for(let f of c)l??=gd(i,n,o),W_(i,n,f,r,r,l)}}function w(t=1){return p_(t)}function GN(t,n){let e=null,i=yM(t);for(let r=0;r<n.length;r++){let o=n[r];if(o==="*"){e=r;continue}if(i===null?Ky(t,o,!0):DM(i,o))return r}return e}function Pe(t){let n=le()[kt][pt];if(!n.projection){let e=t?t.length:1,i=n.projection=Hb(e,null),r=i.slice(),o=n.child;for(;o!==null;){if(o.type!==128){let s=t?GN(o,t):0;s!==null&&(r[s]?r[s].projectionNext=o:i[s]=o,r[s]=o)}o=o.next}}}function he(t,n=0,e,i,r,o){let s=le(),a=He(),l=i?t+1:null;l!==null&&qo(s,a,l,i,r,o,null,e);let c=Xo(a,Ye+t,16,null,e||null);c.projection===null&&(c.projection=n),Zh();let f=!s[Or]||$h();s[kt][pt].projection[c.projection]===null&&l!==null?WN(s,a,l):f&&!kd(c)&&YM(a,s,c)}function WN(t,n,e){let i=Ye+e,r=n.data[i],o=t[i],s=Sd(o,r.tView.ssrId),a=Ta(t,r,void 0,{dehydratedView:s});Na(o,a,0,Go(r,s))}function zn(t,n,e,i){return US(t,n,e,i),zn}function Ue(t,n,e){return HS(t,n,e),Ue}function ce(t){let n=le(),e=He(),i=Jc();ma(i+1);let r=Tp(e,i);if(t.dirty&&Qb(n)===((r.metadata.flags&2)===2)){if(r.matches===null)t.reset([]);else{let o=$S(n,i);t.reset(o,xy),t.notifyOnChanges()}return!0}return!1}function de(){return Mp(le(),Jc())}function Wd(t,n,e,i,r){return WS(n,US(t,e,i,r)),Wd}function $i(t,n,e,i){return WS(t,HS(n,e,i)),$i}function Gi(t=1){ma(Jc()+t)}function ht(t){let n=s_();return Kb(n,Ye+t)}function ud(t,n){return t<<17|n<<2}function Gr(t){return t>>17&32767}function qN(t){return(t&2)==2}function YN(t,n){return t&131071|n<<17}function Km(t){return t|2}function Yo(t){return(t&131068)>>2}function hm(t,n){return t&-131069|n<<2}function ZN(t){return(t&1)===1}function Qm(t){return t|1}function XN(t,n,e,i,r,o){let s=o?n.classBindings:n.styleBindings,a=Gr(s),l=Yo(s);t[i]=e;let c=!1,u;if(Array.isArray(e)){let f=e;u=f[1],(u===null||Oo(f,u)>0)&&(c=!0)}else u=e;if(r)if(l!==0){let h=Gr(t[a+1]);t[i+1]=ud(h,a),h!==0&&(t[h+1]=hm(t[h+1],i)),t[a+1]=YN(t[a+1],i)}else t[i+1]=ud(a,0),a!==0&&(t[a+1]=hm(t[a+1],i)),a=i;else t[i+1]=ud(l,0),a===0?a=i:t[l+1]=hm(t[l+1],i),l=i;c&&(t[i+1]=Km(t[i+1])),oy(t,u,i,!0),oy(t,u,i,!1),KN(n,u,t,i,o),s=ud(a,l),o?n.classBindings=s:n.styleBindings=s}function KN(t,n,e,i,r){let o=r?t.residualClasses:t.residualStyles;o!=null&&typeof n=="string"&&Oo(o,n)>=0&&(e[i+1]=Qm(e[i+1]))}function oy(t,n,e,i){let r=t[e+1],o=n===null,s=i?Gr(r):Yo(r),a=!1;for(;s!==0&&(a===!1||o);){let l=t[s],c=t[s+1];QN(l,n)&&(a=!0,t[s+1]=i?Qm(c):Km(c)),s=i?Gr(c):Yo(c)}a&&(t[e+1]=i?Km(r):Qm(r))}function QN(t,n){return t===null||n==null||(Array.isArray(t)?t[1]:t)===n?!0:Array.isArray(t)&&typeof n=="string"?Oo(t,n)>=0:!1}var pn={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function JN(t){return t.substring(pn.key,pn.keyEnd)}function ek(t){return tk(t),t0(t,n0(t,0,pn.textEnd))}function t0(t,n){let e=pn.textEnd;return e===n?-1:(n=pn.keyEnd=nk(t,pn.key=n,e),n0(t,n,e))}function tk(t){pn.key=0,pn.keyEnd=0,pn.value=0,pn.valueEnd=0,pn.textEnd=t.length}function n0(t,n,e){for(;n<e&&t.charCodeAt(n)<=32;)n++;return n}function nk(t,n,e){for(;n<e&&t.charCodeAt(n)>32;)n++;return n}function Jt(t,n,e){return i0(t,n,e,!1),Jt}function z(t,n){return i0(t,n,null,!0),z}function wt(t){rk(dk,ik,t,!0)}function ik(t,n){for(let e=ek(n);e>=0;e=t0(n,e))Gc(t,JN(n),!0)}function i0(t,n,e,i){let r=le(),o=He(),s=Kh(2);if(o.firstUpdatePass&&o0(o,t,s,i),n!==bn&&zi(r,s,n)){let a=o.data[ri()];s0(o,a,r,r[Fe],t,r[s+1]=fk(n,e),i,s)}}function rk(t,n,e,i){let r=He(),o=Kh(2);r.firstUpdatePass&&o0(r,null,o,i);let s=le();if(e!==bn&&zi(s,o,e)){let a=r.data[ri()];if(a0(a,i)&&!r0(r,o)){let l=i?a.classesWithoutHost:a.stylesWithoutHost;l!==null&&(e=Vc(l,e||"")),Xm(r,a,s,e,i)}else uk(r,a,s,s[Fe],s[o+1],s[o+1]=ck(t,n,e),i,o)}}function r0(t,n){return n>=t.expandoStartIndex}function o0(t,n,e,i){let r=t.data;if(r[e+1]===null){let o=r[ri()],s=r0(t,e);a0(o,i)&&n===null&&!s&&(n=!1),n=ok(r,o,n,i),XN(r,o,n,e,s,i)}}function ok(t,n,e,i){let r=u_(t),o=i?n.residualClasses:n.residualStyles;if(r===null)(i?n.classBindings:n.styleBindings)===0&&(e=mm(null,t,n,e,i),e=wa(e,n.attrs,i),o=null);else{let s=n.directiveStylingLast;if(s===-1||t[s]!==r)if(e=mm(r,t,n,e,i),o===null){let l=sk(t,n,i);l!==void 0&&Array.isArray(l)&&(l=mm(null,t,n,l[1],i),l=wa(l,n.attrs,i),ak(t,n,i,l))}else o=lk(t,n,i)}return o!==void 0&&(i?n.residualClasses=o:n.residualStyles=o),e}function sk(t,n,e){let i=e?n.classBindings:n.styleBindings;if(Yo(i)!==0)return t[Gr(i)]}function ak(t,n,e,i){let r=e?n.classBindings:n.styleBindings;t[Gr(r)]=i}function lk(t,n,e){let i,r=n.directiveEnd;for(let o=1+n.directiveStylingLast;o<r;o++){let s=t[o].hostAttrs;i=wa(i,s,e)}return wa(i,n.attrs,e)}function mm(t,n,e,i,r){let o=null,s=e.directiveEnd,a=e.directiveStylingLast;for(a===-1?a=e.directiveStart:a++;a<s&&(o=n[a],i=wa(i,o.hostAttrs,r),o!==t);)a++;return t!==null&&(e.directiveStylingLast=a),i}function wa(t,n,e){let i=e?1:2,r=-1;if(n!==null)for(let o=0;o<n.length;o++){let s=n[o];typeof s=="number"?r=s:r===i&&(Array.isArray(t)||(t=t===void 0?[]:["",t]),Gc(t,s,e?!0:n[++o]))}return t===void 0?null:t}function ck(t,n,e){if(e==null||e==="")return St;let i=[],r=vn(e);if(Array.isArray(r))for(let o=0;o<r.length;o++)t(i,r[o],!0);else if(r instanceof Set)for(let o of r)t(i,o,!0);else if(typeof r=="object")for(let o in r)Object.hasOwn(r,o)&&t(i,o,r[o]);else typeof r=="string"&&n(i,r);return i}function dk(t,n,e){let i=String(n);i!==""&&!i.includes(" ")&&Gc(t,i,e)}function uk(t,n,e,i,r,o,s,a){r===bn&&(r=St);let l=0,c=0,u=0<r.length?r[0]:null,f=0<o.length?o[0]:null;for(;u!==null||f!==null;){let h=l<r.length?r[l+1]:void 0,p=c<o.length?o[c+1]:void 0,S=null,C;u===f?(l+=2,c+=2,h!==p&&(S=f,C=p)):f===null||u!==null&&u<f?(l+=2,S=u):(c+=2,S=f,C=p),S!==null&&s0(t,n,e,i,S,C,s,a),u=l<r.length?r[l]:null,f=c<o.length?o[c]:null}}function s0(t,n,e,i,r,o,s,a){if(!(n.type&3))return;let l=t.data,c=l[a+1],u=ZN(c)?sy(l,n,e,r,Yo(c),s):void 0;if(!Nd(u)){Nd(o)||qN(c)&&(o=sy(l,null,e,r,a,s));let f=Lh(ri(),e);XM(i,s,f,r,o)}}function sy(t,n,e,i,r,o){let s=n===null,a;for(;r>0;){let l=t[r],c=Array.isArray(l),u=c?l[1]:l,f=u===null,h=e[r+1];h===bn&&(h=f?St:void 0);let p=f?Wc(h,i):u===i?h:void 0;if(c&&!Nd(p)&&(p=Wc(l,i)),Nd(p)&&(a=p,s))return a;let S=t[r+1];r=s?Gr(S):Yo(S)}if(n!==null){let l=o?n.residualClasses:n.residualStyles;l!=null&&(a=Wc(l,i))}return a}function Nd(t){return t!==void 0}function fk(t,n){return t==null||t===""||(typeof n=="string"?t=vn(t)+n:typeof t=="object"&&(t=ra(vn(t)))),t}function a0(t,n){return(t.flags&(n?8:16))!==0}function y(t,n=""){let e=le(),i=He(),r=t+Ye,o=i.firstCreatePass?Xo(i,r,1,n,null):i.data[r],s=hk(i,e,o,n);e[r]=s,rd()&&_p(i,e,s,o),Bo(o,!1)}var hk=(t,n,e,i)=>(od(!0),cM(n[Fe],i));function mk(t,n,e,i=""){return zi(t,Hr(),e)?n+Uc(e)+i:bn}function Te(t){return At("",t),Te}function At(t,n,e){let i=le(),r=mk(i,t,n,e);return r!==bn&&pk(i,ri(),r),At}function pk(t,n,e){let i=Lh(n,t);dM(t[Fe],i,e)}function ay(t,n,e){let i=He();i.firstCreatePass&&l0(n,i.data,i.blueprint,fn(t),e)}function l0(t,n,e,i,r){if(t=it(t),Array.isArray(t))for(let o=0;o<t.length;o++)l0(t[o],n,e,i,r);else{let o=He(),s=le(),a=vt(),l=Nr(t)?t:it(t.provide),c=kh(t),u=a.providerIndexes&1048575,f=a.directiveStart,h=a.providerIndexes>>20;if(Nr(t)||!t.multi){let p=new $r(c,r,ft,null),S=gm(l,n,r?u:u+h,f);S===-1?(_m(yd(a,s),o,l),pm(o,t,n.length),n.push(l),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),e.push(p),s.push(p)):(e[S]=p,s[S]=p)}else{let p=gm(l,n,u+h,f),S=gm(l,n,u,u+h),C=p>=0&&e[p],$=S>=0&&e[S];if(r&&!$||!r&&!C){_m(yd(a,s),o,l);let F=bk(r?vk:gk,e.length,r,i,c,t);!r&&$&&(e[S].providerFactory=F),pm(o,t,n.length,0),n.push(l),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),e.push(F),s.push(F)}else{let F=c0(e[r?S:p],c,!r&&i);pm(o,t,p>-1?p:S,F)}!r&&i&&$&&e[S].componentProviders++}}}function pm(t,n,e,i){let r=Nr(n),o=qb(n);if(r||o){let l=(o?it(n.useClass):n).prototype.ngOnDestroy;if(l){let c=t.destroyHooks||(t.destroyHooks=[]);if(!r&&n.multi){let u=c.indexOf(e);u===-1?c.push(e,[i,l]):c[u+1].push(i,l)}else c.push(e,l)}}}function c0(t,n,e){return e&&t.componentProviders++,t.multi.push(n)-1}function gm(t,n,e,i){for(let r=e;r<i;r++)if(n[r]===t)return r;return-1}function gk(t,n,e,i,r){return Jm(this.multi,[])}function vk(t,n,e,i,r){let o=this.multi,s;if(this.providerFactory){let a=this.providerFactory.componentProviders,l=ya(i,i[Z],this.providerFactory.index,r);s=l.slice(0,a),Jm(o,s);for(let c=a;c<l.length;c++)s.push(l[c])}else s=[],Jm(o,s);return s}function Jm(t,n){for(let e=0;e<t.length;e++){let i=t[e];n.push(i())}return n}function bk(t,n,e,i,r,o){let s=new $r(t,e,ft,null);return s.multi=[],s.index=n,s.componentProviders=0,c0(s,r,i&&!e),s}function Be(t,n){return e=>{e.providersResolver=(i,r)=>ay(i,r?r(t):t,!1),n&&(e.viewProvidersResolver=(i,r)=>ay(i,r?r(n):n,!0))}}function Vp(t,n){return jd(t,n)}var d0=(()=>{class t{applicationErrorHandler=d(Yt);appRef=d(Ft);taskService=d(oi);ngZone=d(N);zonelessEnabled=d(ga);tracing=d(Un,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new pe;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(na):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(d(sm,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let e=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(e);return}this.switchToMicrotaskScheduler(),this.taskService.remove(e)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let e=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(e)})})}notify(e){if(!this.zonelessEnabled&&e===5)return;switch(e){case 0:case 2:{this.appRef.dirtyFlags|=2;break}case 3:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?__:nm;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(na+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let e=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(e),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let e=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();function u0(){return[{provide:Rn,useExisting:d0},{provide:N,useClass:ia},{provide:ga,useValue:!0}]}var Bp=(()=>{class t{compileModuleSync(e){return new Id(e)}compileModuleAsync(e){return Promise.resolve(this.compileModuleSync(e))}clearCache(){}clearCacheFor(e){}getModuleId(e){}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();function _k(){return typeof $localize<"u"&&$localize.locale||Ra}var qd=new b("",{factory:()=>d(qd,{optional:!0,skipSelf:!0})||_k()});function Q(t,n){return Hs(t,n?.equal)}function Ne(t){return eb(t)}var f0=class t extends Error{_brand;constructor(n){super(n)}static IDLE=new t("IDLE");static LOADING=new t("LOADING")};function _0(t,n){let e=Object.create(dy);e.value=t,e.transformFn=n?.transform;function i(){if(vr(e),e.value===ep){let r=null;throw new x(-950,r)}return e.value}return i[et]=e,i}var _n=class{attributeName;constructor(n){this.attributeName=n}__NG_ELEMENT_ID__=()=>np(this.attributeName);toString(){return`HostAttributeToken ${this.attributeName}`}};function zp(t){return Lk(t)?t.default:t}function Lk(t){return t&&typeof t=="object"&&"default"in t}function h0(t,n){return _0(t,n)}function Vk(t){return _0(ep,t)}var yn=(h0.required=Vk,h0);function m0(t,n){return Np(n)}function Bk(t,n){return kp(n)}var ui=(m0.required=Bk,m0);function p0(t,n){return Np(n)}function jk(t,n){return kp(n)}var y0=(p0.required=jk,p0);var Hk=1e4;var Rq=Hk-1e3;var Oe=(()=>{class t{static __NG_ELEMENT_ID__=Uk}return t})();function Uk(t){return zk(vt(),le(),(t&16)===16)}function zk(t,n,e){if(ii(t)&&!e){let i=qt(t.index,n);return new ji(i,i)}else if(t.type&175){let i=n[kt];return new ji(i,n)}return null}var Hp=new b(""),$k=new b("");function Oa(t){return!t.moduleRef}function Gk(t){let n=Oa(t)?t.r3Injector:t.moduleRef.injector,e=n.get(N);return e.run(()=>{Oa(t)?t.r3Injector.resolveInjectorInitializers():t.moduleRef.resolveInjectorInitializers();let i=n.get(Yt),r;if(e.runOutsideAngular(()=>{r=e.onError.subscribe({next:i})}),Oa(t)){let o=()=>n.destroy(),s=t.platformInjector.get(Hp);s.add(o),n.onDestroy(()=>{r.unsubscribe(),s.delete(o)})}else{let o=()=>t.moduleRef.destroy(),s=t.platformInjector.get(Hp);s.add(o),t.moduleRef.onDestroy(()=>{_a(t.allPlatformModules,t.moduleRef),r.unsubscribe(),s.delete(o)})}return qk(i,e,()=>{let o=n.get(oi),s=o.add(),a=n.get(Rp);return a.runInitializers(),a.donePromise.then(()=>{let l=n.get(qd,Ra);if(e0(l||Ra),!n.get($k,!0))return Oa(t)?n.get(Ft):(t.allPlatformModules.push(t.moduleRef),t.moduleRef);if(Oa(t)){let u=n.get(Ft);return t.rootComponent!==void 0&&u.bootstrap(t.rootComponent),u}else return Wk?.(t.moduleRef,t.allPlatformModules),t.moduleRef}).finally(()=>{o.remove(s)})})})}var Wk;function qk(t,n,e){try{let i=e();return di(i)?i.catch(r=>{throw n.runOutsideAngular(()=>t(r)),r}):i}catch(i){throw n.runOutsideAngular(()=>t(i)),i}}var Yd=null;function Yk(t=[],n){return R.create({name:n,providers:[{provide:da,useValue:"platform"},{provide:Hp,useValue:new Set([()=>Yd=null])},...t]})}function Zk(t=[]){if(Yd)return Yd;let n=Yk(t);return Yd=n,QS(),Xk(n),n}function Xk(t){let n=t.get(sd,null);ot(t,()=>{n?.forEach(e=>e())})}function S0(t){let{rootComponent:n,appProviders:e,platformProviders:i,platformRef:r}=t;Ee(_e.BootstrapApplicationStart);try{let o=r?.injector??Zk(i),s=[u0(),S_,...e||[]],a=new Ea({providers:s,parent:o,debugName:"",runEnvironmentInitializers:!1});return Gk({r3Injector:a.injector,platformInjector:o,rootComponent:n})}catch(o){return Promise.reject(o)}finally{Ee(_e.BootstrapApplicationEnd)}}function P(t){return typeof t=="boolean"?t:t!=null&&t!=="false"}function Yr(t,n=NaN){return!isNaN(parseFloat(t))&&!isNaN(Number(t))?Number(t):n}var jp=Symbol("NOT_SET"),C0=new Set,Kk=k(v({},Eo),{kind:"afterRenderEffectPhase",consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,value:jp,cleanup:null,consumerMarkedDirty(){if(this.sequence.impl.executing){if(this.sequence.lastPhase===null||this.sequence.lastPhase<this.phase)return;this.sequence.erroredOrDestroyed=!0}this.sequence.scheduler.notify(7)},phaseFn(t){if(this.sequence.lastPhase=this.phase,!this.dirty)return this.signal;if(this.dirty=!1,this.value!==jp&&!Co(this))return this.signal;try{for(let r of this.cleanup??C0)r()}finally{this.cleanup?.clear()}let n=[];t!==void 0&&n.push(t),n.push(this.registerCleanupFn);let e=Ii(this),i;try{i=this.userFn.apply(null,n)}finally{br(this,e)}return(this.value===jp||!this.equal(this.value,i))&&(this.value=i,this.version++),this.signal}}),Up=class extends Sa{scheduler;lastPhase=null;nodes=[void 0,void 0,void 0,void 0];onDestroyFns=null;constructor(n,e,i,r,o,s=null){super(n,[void 0,void 0,void 0,void 0],i,!1,o.get(We),s),this.scheduler=r;for(let a of pp){let l=e[a];if(l===void 0)continue;let c=Object.create(Kk);c.sequence=this,c.phase=a,c.userFn=l,c.dirty=!0,c.signal=()=>(vr(c),c.value),c.signal[et]=c,c.registerCleanupFn=u=>(c.cleanup??=new Set).add(u),this.nodes[a]=c,this.hooks[a]=u=>c.phaseFn(u)}}afterRun(){super.afterRun(),this.lastPhase=null}destroy(){if(this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();super.destroy();for(let n of this.nodes)if(n)try{for(let e of n.cleanup??C0)e()}finally{Mi(n)}}};function Zr(t,n){let e=n?.injector??d(R),i=e.get(Rn),r=e.get(Od),o=e.get(Un,null,{optional:!0});r.impl??=e.get(gp);let s=t;typeof s=="function"&&(s={mixedReadWrite:t});let a=e.get(jo,null,{optional:!0}),l=new Up(r.impl,[s.earlyRead,s.write,s.mixedReadWrite,s.read],a?.view,i,e,o?.snapshot(null));return r.impl.register(l),l}function Zd(t,n){let e=ki(t),i=n.elementInjector||Po();return new Wo(e).create(i,n.projectableNodes,n.hostElement,n.environmentInjector,n.directives,n.bindings)}var D0=null;function en(){return D0}function $p(t){D0??=t}var Pa=class{},Qo=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=G({token:t,factory:()=>d(E0),providedIn:"platform"})}return t})();var E0=(()=>{class t extends Qo{_location;_history;_doc=d(L);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return en().getBaseHref(this._doc)}onPopState(e){let i=en().getGlobalEventTarget(this._doc,"window");return i.addEventListener("popstate",e,!1),()=>i.removeEventListener("popstate",e)}onHashChange(e){let i=en().getGlobalEventTarget(this._doc,"window");return i.addEventListener("hashchange",e,!1),()=>i.removeEventListener("hashchange",e)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(e){this._location.pathname=e}pushState(e,i,r){this._history.pushState(e,i,r)}replaceState(e,i,r){this._history.replaceState(e,i,r)}forward(){this._history.forward()}back(){this._history.back()}historyGo(e=0){this._history.go(e)}getState(){return this._history.state}static \u0275fac=function(i){return new(i||t)};static \u0275prov=G({token:t,factory:()=>new t,providedIn:"platform"})}return t})();function I0(t,n){return t?n?t.endsWith("/")?n.startsWith("/")?t+n.slice(1):t+n:n.startsWith("/")?t+n:`${t}/${n}`:t:n}function w0(t){let n=t.search(/#|\?|$/);return t[n-1]==="/"?t.slice(0,n-1)+t.slice(n):t}function Wi(t){return t&&t[0]!=="?"?`?${t}`:t}var Xd=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=G({token:t,factory:()=>d(Jk),providedIn:"root"})}return t})(),Qk=new b(""),Jk=(()=>{class t extends Xd{_platformLocation;_baseHref;_removeListenerFns=[];constructor(e,i){super(),this._platformLocation=e,this._baseHref=i??this._platformLocation.getBaseHrefFromDOM()??d(L).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(e){this._removeListenerFns.push(this._platformLocation.onPopState(e),this._platformLocation.onHashChange(e))}getBaseHref(){return this._baseHref}prepareExternalUrl(e){return I0(this._baseHref,e)}path(e=!1){let i=this._platformLocation.pathname+Wi(this._platformLocation.search),r=this._platformLocation.hash;return r&&e?`${i}${r}`:i}pushState(e,i,r,o){let s=this.prepareExternalUrl(r+Wi(o));this._platformLocation.pushState(e,i,s)}replaceState(e,i,r,o){let s=this.prepareExternalUrl(r+Wi(o));this._platformLocation.replaceState(e,i,s)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(e=0){this._platformLocation.historyGo?.(e)}static \u0275fac=function(i){return new(i||t)(A(Qo),A(Qk,8))};static \u0275prov=G({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var qi=(()=>{class t{_subject=new E;_basePath;_locationStrategy;_urlChangeListeners=[];_urlChangeSubscription=null;constructor(e){this._locationStrategy=e;let i=this._locationStrategy.getBaseHref();this._basePath=nA(w0(x0(i))),this._locationStrategy.onPopState(r=>{let o={url:this.path(!0),pop:!0,state:r.state,type:r.type};r.hasUAVisualTransition&&(o.hasUAVisualTransition=!0),this._subject.next(o)})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(e=!1){return this.normalize(this._locationStrategy.path(e))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(e,i=""){return this.path()==this.normalize(e+Wi(i))}normalize(e){return t.stripTrailingSlash(tA(this._basePath,x0(e)))}prepareExternalUrl(e){return e&&e[0]!=="/"&&(e="/"+e),this._locationStrategy.prepareExternalUrl(e)}go(e,i="",r=null){this._locationStrategy.pushState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+Wi(i)),r)}replaceState(e,i="",r=null){this._locationStrategy.replaceState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+Wi(i)),r)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(e=0){this._locationStrategy.historyGo?.(e)}onUrlChange(e){return this._urlChangeListeners.push(e),this._urlChangeSubscription??=this.subscribe(i=>{this._notifyUrlChangeListeners(i.url,i.state)}),()=>{let i=this._urlChangeListeners.indexOf(e);this._urlChangeListeners.splice(i,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(e="",i){this._urlChangeListeners.forEach(r=>r(e,i))}subscribe(e,i,r){return this._subject.subscribe({next:e,error:i??void 0,complete:r??void 0})}static normalizeQueryParams=Wi;static joinWithSlash=I0;static stripTrailingSlash=w0;static \u0275fac=function(i){return new(i||t)(A(Xd))};static \u0275prov=G({token:t,factory:()=>eA(),providedIn:"root"})}return t})();function eA(){return new qi(A(Xd))}function tA(t,n){if(!t||!n.startsWith(t))return n;let e=n.substring(t.length);return e===""||["/",";","?","#"].includes(e[0])?e:n}function x0(t){return t.replace(/\/index\.html$/,"")}function nA(t){if(new RegExp("^(https?:)?//").test(t)){let[,e]=t.split(/\/\/[^\/]+/);return e}return t}var Gp=(()=>{class t{_viewContainerRef;_viewRef=null;ngTemplateOutletContext=null;ngTemplateOutlet=null;ngTemplateOutletInjector=null;injector=d(R);constructor(e){this._viewContainerRef=e}ngOnChanges(e){if(this._shouldRecreateView(e)){let i=this._viewContainerRef;if(this._viewRef&&i.remove(i.indexOf(this._viewRef)),!this.ngTemplateOutlet){this._viewRef=null;return}let r=this._createContextForwardProxy();this._viewRef=i.createEmbeddedView(this.ngTemplateOutlet,r,{injector:this._getInjector()})}}_getInjector(){return this.ngTemplateOutletInjector==="outlet"?this.injector:this.ngTemplateOutletInjector??void 0}_shouldRecreateView(e){return!!e.ngTemplateOutlet||!!e.ngTemplateOutletInjector}_createContextForwardProxy(){return new Proxy({},{set:(e,i,r)=>this.ngTemplateOutletContext?Reflect.set(this.ngTemplateOutletContext,i,r):!1,get:(e,i,r)=>{if(this.ngTemplateOutletContext)return Reflect.get(this.ngTemplateOutletContext,i,r)}})}static \u0275fac=function(i){return new(i||t)(ft(bt))};static \u0275dir=H({type:t,selectors:[["","ngTemplateOutlet",""]],inputs:{ngTemplateOutletContext:"ngTemplateOutletContext",ngTemplateOutlet:"ngTemplateOutlet",ngTemplateOutletInjector:"ngTemplateOutletInjector"},features:[at]})}return t})();function Fa(t,n){n=encodeURIComponent(n);for(let e of t.split(";")){let i=e.indexOf("="),[r,o]=i==-1?[e,""]:[e.slice(0,i),e.slice(i+1)];if(r.trim()!==n)continue;let s=o;try{s=decodeURIComponent(o)}catch{}return s.length>1&&s[0]==='"'&&s[s.length-1]==='"'&&(s=s.slice(1,-1)),s}return null}var Wp="browser";function M0(t){return t===Wp}var La=class{_doc;constructor(n){this._doc=n}manager},Kd=(()=>{class t extends La{constructor(e){super(e)}supports(e){return!0}addEventListener(e,i,r,o){return e.addEventListener(i,r,o),()=>this.removeEventListener(e,i,r,o)}removeEventListener(e,i,r,o){return e.removeEventListener(i,r,o)}static \u0275fac=function(i){return new(i||t)(A(L))};static \u0275prov=G({token:t,factory:t.\u0275fac})}return t})(),eu=new b(""),Xp=(()=>{class t{_zone;_plugins;_eventNameToPlugin=new Map;constructor(e,i){this._zone=i,e.forEach(s=>{s.manager=this});let r=e.filter(s=>!(s instanceof Kd));this._plugins=r.slice().reverse();let o=e.find(s=>s instanceof Kd);o&&this._plugins.push(o)}addEventListener(e,i,r,o){return this._findPluginFor(i).addEventListener(e,i,r,o)}getZone(){return this._zone}_findPluginFor(e){let i=this._eventNameToPlugin.get(e);if(i)return i;if(i=this._plugins.find(o=>o.supports(e)),!i)throw new x(-5101,!1);return this._eventNameToPlugin.set(e,i),i}static \u0275fac=function(i){return new(i||t)(A(eu),A(N))};static \u0275prov=G({token:t,factory:t.\u0275fac})}return t})(),qp="ng-app-id";function T0(t){for(let n of t)n.remove()}function N0(t,n){let e=n.createElement("style");return e.textContent=t,e}function aA(t,n,e,i){let r=t.head?.querySelectorAll(`style[${qp}="${n}"],link[${qp}="${n}"]`);if(!r||r.length===0)return!1;for(let o of r)o.removeAttribute(qp),o instanceof HTMLLinkElement?i.set(o.href.slice(o.href.lastIndexOf("/")+1),{usage:0,elements:[o]}):o.textContent&&e.set(o.textContent,{usage:0,elements:[o]});return!0}function Zp(t,n){let e=n.createElement("link");return e.setAttribute("rel","stylesheet"),e.setAttribute("href",t),e}var Kp=(()=>{class t{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(e,i,r,o={}){this.doc=e,this.appId=i,this.nonce=r,aA(e,i,this.inline,this.external)&&this.hosts.add(e.head)}addStyles(e,i){for(let r of e)this.addUsage(r,this.inline,N0);i?.forEach(r=>this.addUsage(r,this.external,Zp))}removeStyles(e,i){for(let r of e)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(e,i,r){let o=i.get(e);o?o.usage++:i.set(e,{usage:1,elements:[...this.hosts].map(s=>this.addElement(s,r(e,this.doc)))})}removeUsage(e,i){let r=i.get(e);r&&(r.usage--,r.usage<=0&&(T0(r.elements),i.delete(e)))}ngOnDestroy(){for(let[,{elements:e}]of[...this.inline,...this.external])T0(e);this.hosts.clear()}addHost(e){if(!this.hosts.has(e)){this.hosts.add(e);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(e,N0(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(e,Zp(i,this.doc)))}}removeHost(e){this.hosts.delete(e);for(let i of[...this.inline.values(),...this.external.values()]){let r=[];for(let o of i.elements)o.parentNode===e?o.remove():r.push(o);i.elements=r}}addElement(e,i){return this.nonce&&i.setAttribute("nonce",this.nonce),e.appendChild(i)}static \u0275fac=function(i){return new(i||t)(A(L),A(si),A(Li,8),A(Ur))};static \u0275prov=G({token:t,factory:t.\u0275fac})}return t})(),Yp={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},Qp=/%COMP%/g;var A0="%COMP%",lA=`_nghost-${A0}`,cA=`_ngcontent-${A0}`,dA=!0,uA=new b("",{factory:()=>dA}),fA=new b("");function hA(t){return cA.replace(Qp,t)}function mA(t){return lA.replace(Qp,t)}function R0(t,n){return n.map(e=>e.replace(Qp,t))}var Jp=(()=>{class t{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;cssVarNamespace;constructor(e,i,r,o,s,a,l=null,c=null,u=null){this.eventManager=e,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=o,this.doc=s,this.ngZone=a,this.nonce=l,this.tracingService=c,this.cssVarNamespace=u??"",this.defaultRenderer=new Va(e,s,a,this.tracingService,this.cssVarNamespace)}createRenderer(e,i){if(!e||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(e,i);return r instanceof Jd?r.applyToHost(e):r instanceof Ba&&r.applyStyles(),r}getOrCreateRenderer(e,i){let r=this.rendererByCompId,o=r.get(i.id);if(!o){let s=this.doc,a=this.ngZone,l=this.eventManager,c=this.sharedStylesHost,u=this.removeStylesOnCompDestroy,f=this.tracingService;switch(i.encapsulation){case gn.Emulated:o=new Jd(l,c,i,this.appId,u,s,a,f,this.cssVarNamespace);break;case gn.ShadowDom:return new Qd(l,e,i,s,a,this.nonce,f,this.cssVarNamespace,c);case gn.ExperimentalIsolatedShadowDom:return new Qd(l,e,i,s,a,this.nonce,f,this.cssVarNamespace);default:o=new Ba(l,c,i,u,s,a,f,this.cssVarNamespace);break}r.set(i.id,o)}return o}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(e){this.rendererByCompId.delete(e)}static \u0275fac=function(i){return new(i||t)(A(Xp),A(Wr),A(si),A(uA),A(L),A(N),A(Li),A(Un,8),A(fA,8))};static \u0275prov=G({token:t,factory:t.\u0275fac})}return t})(),Va=class{eventManager;doc;ngZone;tracingService;cssVarNamespace;data=Object.create(null);throwOnSyntheticProps=!0;constructor(n,e,i,r,o=""){this.eventManager=n,this.doc=e,this.ngZone=i,this.tracingService=r,this.cssVarNamespace=o}destroy(){}destroyNode=null;createElement(n,e){return e?this.doc.createElementNS(Yp[e]||e,n):this.doc.createElement(n)}createComment(n){return this.doc.createComment(n)}createText(n){return this.doc.createTextNode(n)}appendChild(n,e){(k0(n)?n.content:n).appendChild(e)}insertBefore(n,e,i){if(n){let r=k0(n)?n.content:n;if(i!=null&&i.parentNode!==r)throw new x(-5106,!1);r.insertBefore(e,i)}}removeChild(n,e){e.remove()}selectRootElement(n,e){let i=typeof n=="string"?this.doc.querySelector(n):n;if(!i)throw new x(-5104,!1);return e||(i.textContent=""),i}parentNode(n){return n.parentNode}nextSibling(n){return n.nextSibling}setAttribute(n,e,i,r){if(r){e=r+":"+e;let o=Yp[r];o?n.setAttributeNS(o,e,i):n.setAttribute(e,i)}else n.setAttribute(e,i)}removeAttribute(n,e,i){if(i){let r=Yp[i];r?n.removeAttributeNS(r,e):n.removeAttribute(`${i}:${e}`)}else n.removeAttribute(e)}addClass(n,e){n.classList.add(e)}removeClass(n,e){n.classList.remove(e)}setStyle(n,e,i,r){let o=e.startsWith("--");o&&(e=e.replace("%NS%",this.cssVarNamespace)),o||r&(jn.DashCase|jn.Important)?n.style.setProperty(e,i,r&jn.Important?"important":""):n.style[e]=i}removeStyle(n,e,i){let r=e.startsWith("--");r&&(e=e.replace("%NS%",this.cssVarNamespace)),r||i&jn.DashCase?n.style.removeProperty(e):n.style[e]=""}setProperty(n,e,i){n!=null&&(n[e]=i)}setValue(n,e){n.nodeValue=e}listen(n,e,i,r){if(typeof n=="string"&&(n=en().getGlobalEventTarget(this.doc,n),!n))throw new x(-5102,!1);let o=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(o=this.tracingService.wrapEventListener(n,e,o)),this.eventManager.addEventListener(n,e,o,r)}decoratePreventDefault(n){return e=>{if(e==="__ngUnwrap__")return n;n(e)===!1&&e.preventDefault()}}};function k0(t){return t.tagName==="TEMPLATE"&&t.content!==void 0}var Qd=class extends Va{hostEl;sharedStylesHost;shadowRoot;constructor(n,e,i,r,o,s,a,l,c){super(n,r,o,a,l),this.hostEl=e,this.sharedStylesHost=c,this.shadowRoot=e.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let u=i.styles;u=R0(i.id,u).map(h=>h.replace(/%NS%/g,l));for(let h of u){let p=document.createElement("style");s&&p.setAttribute("nonce",s),p.textContent=h,this.shadowRoot.appendChild(p)}let f=i.getExternalStyles?.();if(f)for(let h of f){let p=Zp(h,r);s&&p.setAttribute("nonce",s),this.shadowRoot.appendChild(p)}}nodeOrShadowRoot(n){return n===this.hostEl?this.shadowRoot:n}appendChild(n,e){return super.appendChild(this.nodeOrShadowRoot(n),e)}insertBefore(n,e,i){return super.insertBefore(this.nodeOrShadowRoot(n),e,i)}removeChild(n,e){return super.removeChild(null,e)}parentNode(n){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(n)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},Ba=class extends Va{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(n,e,i,r,o,s,a,l,c){super(n,o,s,a,l),this.sharedStylesHost=e,this.removeStylesOnCompDestroy=r;let u=i.styles,f=c?R0(c,u):u;this.styles=f.map(h=>h.replace(/%NS%/g,l)),this.styleUrls=i.getExternalStyles?.(c)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&Bi.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},Jd=class extends Ba{contentAttr;hostAttr;constructor(n,e,i,r,o,s,a,l,c){let u=r+"-"+i.id;super(n,e,i,o,s,a,l,c,u),this.contentAttr=hA(u),this.hostAttr=mA(u)}applyToHost(n){this.applyStyles(),this.setAttribute(n,this.hostAttr,"")}createElement(n,e){let i=super.createElement(n,e);return super.setAttribute(i,this.contentAttr,""),i}};var tu=class t extends Pa{supportsDOMEvents=!0;static makeCurrent(){$p(new t)}onAndCancel(n,e,i,r){return n.addEventListener(e,i,r),()=>{n.removeEventListener(e,i,r)}}dispatchEvent(n,e){n.dispatchEvent(e)}remove(n){n.remove()}createElement(n,e){return e=e||this.getDefaultDocument(),e.createElement(n)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(n){return n.nodeType===Node.ELEMENT_NODE}isShadowRoot(n){return n instanceof DocumentFragment}getGlobalEventTarget(n,e){return e==="window"?window:e==="document"?n:e==="body"?n.body:null}getBaseHref(n){let e=pA();return e==null?null:gA(e)}resetBaseElement(){ja=null}getUserAgent(){return window.navigator.userAgent}getCookie(n){return Fa(document.cookie,n)}},ja=null;function pA(){return ja=ja||document.head.querySelector("base"),ja?ja.getAttribute("href"):null}function gA(t){return new URL(t,document.baseURI).pathname}var O0=["alt","control","meta","shift"],vA={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},bA={alt:t=>t.altKey,control:t=>t.ctrlKey,meta:t=>t.metaKey,shift:t=>t.shiftKey},P0=(()=>{class t extends La{constructor(e){super(e)}supports(e){return t.parseEventName(e)!=null}addEventListener(e,i,r,o){let s=t.parseEventName(i),a=t.eventCallback(s.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>en().onAndCancel(e,s.domEventName,a,o))}static parseEventName(e){let i=e.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let o=t._normalizeKey(i.pop()),s="",a=i.indexOf("code");if(a>-1&&(i.splice(a,1),s="code."),O0.forEach(c=>{let u=i.indexOf(c);u>-1&&(i.splice(u,1),s+=c+".")}),s+=o,i.length!=0||o.length===0)return null;let l={};return l.domEventName=r,l.fullKey=s,l}static matchEventFullKeyCode(e,i){let r=vA[e.key]||e.key,o="";return i.indexOf("code.")>-1&&(r=e.code,o="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),O0.forEach(s=>{if(s!==r){let a=bA[s];a(e)&&(o+=s+".")}}),o+=r,o===i)}static eventCallback(e,i,r){return o=>{t.matchEventFullKeyCode(o,e)&&r.runGuarded(()=>i(o))}}static _normalizeKey(e){return e==="esc"?"escape":e}static \u0275fac=function(i){return new(i||t)(A(L))};static \u0275prov=G({token:t,factory:t.\u0275fac})}return t})();async function eg(t,n,e){let i=v({rootComponent:t},_A(n,e));return S0(i)}function _A(t,n){return{platformRef:n?.platformRef,appProviders:[...EA,...t?.providers??[]],platformProviders:DA}}function yA(){tu.makeCurrent()}function SA(){return new Ot}function CA(){return rp(document),document}var DA=[{provide:Ur,useValue:Wp},{provide:sd,useValue:yA,multi:!0},{provide:L,useFactory:CA}];var EA=[{provide:da,useValue:"root"},{provide:Ot,useFactory:SA},{provide:eu,useClass:Kd,multi:!0},{provide:eu,useClass:P0,multi:!0},Jp,{provide:Wr,useClass:Kp},{provide:Kp,useExisting:Wr},Xp,{provide:Ke,useExisting:Jp},[]];var hi=class t{headers;normalizedNames=new Map;lazyInit;lazyUpdate=null;constructor(n){n?typeof n=="string"?this.lazyInit=()=>{this.headers=new Map,n.split(`
`).forEach(e=>{let i=e.indexOf(":");if(i>0){let r=e.slice(0,i),o=e.slice(i+1).trim();this.addHeaderEntry(r,o)}})}:typeof Headers<"u"&&n instanceof Headers?(this.headers=new Map,n.forEach((e,i)=>{this.addHeaderEntry(i,e)})):this.lazyInit=()=>{this.headers=new Map,Object.entries(n).forEach(([e,i])=>{this.setHeaderEntries(e,i)})}:this.headers=new Map}has(n){return this.init(),this.headers.has(n.toLowerCase())}get(n){this.init();let e=this.headers.get(n.toLowerCase());return e&&e.length>0?e[0]:null}keys(){return this.init(),Array.from(this.normalizedNames.values())}getAll(n){return this.init(),this.headers.get(n.toLowerCase())||null}append(n,e){return this.clone({name:n,value:e,op:"a"})}set(n,e){return this.clone({name:n,value:e,op:"s"})}delete(n,e){return this.clone({name:n,value:e,op:"d"})}maybeSetNormalizedName(n,e){this.normalizedNames.has(e)||this.normalizedNames.set(e,n)}init(){this.lazyInit&&(this.lazyInit instanceof t?this.copyFrom(this.lazyInit):this.lazyInit(),this.lazyInit=null,this.lazyUpdate&&(this.lazyUpdate.forEach(n=>this.applyUpdate(n)),this.lazyUpdate=null))}copyFrom(n){n.init();for(let[e,i]of n.headers.entries())this.headers.set(e,i),this.normalizedNames.set(e,n.normalizedNames.get(e))}clone(n){let e=new t;return e.lazyInit=this.lazyInit&&this.lazyInit instanceof t?this.lazyInit:this,e.lazyUpdate=(this.lazyUpdate||[]).concat([n]),e}applyUpdate(n){let e=n.name.toLowerCase();switch(n.op){case"a":case"s":let i=n.value;if(typeof i=="string"&&(i=[i]),i.length===0)return;this.maybeSetNormalizedName(n.name,e);let r=n.op==="a"?(this.headers.get(e)||[]).slice():[];r.push(...i),this.headers.set(e,r);break;case"d":let o=n.value;if(o===void 0)this.headers.delete(e),this.normalizedNames.delete(e);else{let s=Array.isArray(o)?o:[o],a=this.headers.get(e);if(!a)return;a=a.filter(l=>s.indexOf(l)===-1),a.length===0?(this.headers.delete(e),this.normalizedNames.delete(e)):this.headers.set(e,a)}break}}addHeaderEntry(n,e){let i=n.toLowerCase();this.maybeSetNormalizedName(n,i),this.headers.has(i)?this.headers.get(i).push(e):this.headers.set(i,[e])}setHeaderEntries(n,e){let i=(Array.isArray(e)?e:[e]).map(o=>o.toString()),r=n.toLowerCase();this.headers.set(r,i),this.maybeSetNormalizedName(n,r)}forEach(n){this.init(),Array.from(this.normalizedNames.keys()).forEach(e=>n(this.normalizedNames.get(e),this.headers.get(e)))}};var ig=class{map=new Map;set(n,e){return this.map.set(n,e),this}get(n){return this.map.has(n)||this.map.set(n,n.defaultValue()),this.map.get(n)}delete(n){return this.map.delete(n),this}has(n){return this.map.has(n)}keys(){return this.map.keys()}},rg=class{encodeKey(n){return F0(n)}encodeValue(n){return F0(n)}decodeKey(n){return decodeURIComponent(n)}decodeValue(n){return decodeURIComponent(n)}};function wA(t,n){let e=new Map;return t.length>0&&t.replace(/^\?/,"").split("&").forEach(r=>{let o=r.indexOf("="),[s,a]=o==-1?[n.decodeKey(r),""]:[n.decodeKey(r.slice(0,o)),n.decodeValue(r.slice(o+1))],l=e.get(s)||[];l.push(a),e.set(s,l)}),e}var xA=/%(\d[a-f0-9])/gi,IA={40:"@","3A":":",24:"$","2C":",","3B":";","3D":"=","3F":"?","2F":"/"};function F0(t){return encodeURIComponent(t).replace(xA,(n,e)=>IA[e]??n)}function nu(t){return`${t}`}var fi=class t{map;encoder;updates=null;cloneFrom=null;constructor(n={}){if(this.encoder=n.encoder||new rg,n.fromString){if(n.fromObject)throw new x(2805,!1);this.map=wA(n.fromString,this.encoder)}else n.fromObject?(this.map=new Map,Object.keys(n.fromObject).forEach(e=>{let i=n.fromObject[e],r=Array.isArray(i)?i.map(nu):[nu(i)];this.map.set(e,r)})):this.map=null}has(n){return this.init(),this.map.has(n)}get(n){this.init();let e=this.map.get(n);return e?e[0]:null}getAll(n){return this.init(),this.map.get(n)||null}keys(){return this.init(),Array.from(this.map.keys())}append(n,e){return this.clone({param:n,value:e,op:"a"})}appendAll(n){let e=[];return Object.keys(n).forEach(i=>{let r=n[i];Array.isArray(r)?r.forEach(o=>{e.push({param:i,value:o,op:"a"})}):e.push({param:i,value:r,op:"a"})}),this.clone(e)}set(n,e){return this.clone({param:n,value:e,op:"s"})}delete(n,e){return this.clone({param:n,value:e,op:"d"})}toString(){return this.init(),this.keys().map(n=>{let e=this.encoder.encodeKey(n);return this.map.get(n).map(i=>e+"="+this.encoder.encodeValue(i)).join("&")}).filter(n=>n!=="").join("&")}clone(n){let e=new t({encoder:this.encoder});return e.cloneFrom=this.cloneFrom||this,e.updates=(this.updates||[]).concat(n),e}init(){if(this.map===null&&(this.map=new Map),this.cloneFrom!==null){this.cloneFrom.init();for(let[n,e]of this.cloneFrom.map.entries())this.map.set(n,e);this.updates.forEach(n=>{switch(n.op){case"a":case"s":let e=n.op==="a"?(this.map.get(n.param)||[]).slice():[];e.push(nu(n.value)),this.map.set(n.param,e);break;case"d":if(n.value!==void 0){let i=(this.map.get(n.param)||[]).slice(),r=i.indexOf(nu(n.value));r!==-1&&i.splice(r,1),i.length>0?this.map.set(n.param,i):this.map.delete(n.param)}else{this.map.delete(n.param);break}}}),this.cloneFrom=this.updates=null}}};function MA(t){switch(t){case"DELETE":case"GET":case"HEAD":case"OPTIONS":case"JSONP":return!1;default:return!0}}function L0(t){return typeof ArrayBuffer<"u"&&t instanceof ArrayBuffer}function V0(t){return typeof Blob<"u"&&t instanceof Blob}function B0(t){return typeof FormData<"u"&&t instanceof FormData}function TA(t){return typeof URLSearchParams<"u"&&t instanceof URLSearchParams}var tg="Content-Type",j0="Accept",z0="text/plain",$0="application/json",NA=`${$0}, ${z0}, */*`,Jo=class t{url;body=null;headers;context;reportProgress=!1;reportUploadProgress=!1;reportDownloadProgress=!1;withCredentials=!1;credentials;keepalive=!1;cache;priority;mode;redirect;referrer;integrity;referrerPolicy;responseType="json";method;params;urlWithParams;transferCache;timeout;constructor(n,e,i,r){this.url=e,this.method=n.toUpperCase();let o;if(MA(this.method)||r?(this.body=i!==void 0?i:null,o=r):o=i,o){if(this.reportProgress=!!o.reportProgress,this.reportUploadProgress=!!o.reportUploadProgress,this.reportDownloadProgress=!!o.reportDownloadProgress,this.withCredentials=!!o.withCredentials,this.keepalive=!!o.keepalive,o.responseType&&(this.responseType=o.responseType),o.headers&&(this.headers=o.headers),o.context&&(this.context=o.context),o.params&&(this.params=o.params),o.priority&&(this.priority=o.priority),o.cache&&(this.cache=o.cache),o.credentials&&(this.credentials=o.credentials),typeof o.timeout=="number"){if(o.timeout<1||!Number.isInteger(o.timeout))throw new x(2822,"");this.timeout=o.timeout}o.mode&&(this.mode=o.mode),o.redirect&&(this.redirect=o.redirect),o.integrity&&(this.integrity=o.integrity),o.referrer!==void 0&&(this.referrer=o.referrer),o.referrerPolicy&&(this.referrerPolicy=o.referrerPolicy),this.transferCache=o.transferCache}if(this.headers??=new hi,this.context??=new ig,!this.params)this.params=new fi,this.urlWithParams=e;else{let s=this.params.toString();if(s.length===0)this.urlWithParams=e;else{let a=e,l="",c=e.indexOf("#");c!==-1&&(l=e.substring(c),a=e.substring(0,c));let u=a.indexOf("?"),f=u===-1?"?":u<a.length-1?"&":"";this.urlWithParams=a+f+s+l}}}serializeBody(){return this.body===null?null:typeof this.body=="string"||L0(this.body)||V0(this.body)||B0(this.body)||TA(this.body)?this.body:this.body instanceof fi?this.body.toString():typeof this.body=="object"||typeof this.body=="boolean"||Array.isArray(this.body)?JSON.stringify(this.body):this.body.toString()}detectContentTypeHeader(){return this.body===null||B0(this.body)?null:V0(this.body)?this.body.type||null:L0(this.body)?null:typeof this.body=="string"?z0:this.body instanceof fi?"application/x-www-form-urlencoded;charset=UTF-8":typeof this.body=="object"||typeof this.body=="number"||typeof this.body=="boolean"?$0:null}clone(n={}){let e=n.method||this.method,i=n.url||this.url,r=n.responseType||this.responseType,o=n.keepalive??this.keepalive,s=n.priority||this.priority,a=n.cache||this.cache,l=n.mode||this.mode,c=n.redirect||this.redirect,u=n.credentials||this.credentials,f=n.referrer??this.referrer,h=n.integrity||this.integrity,p=n.referrerPolicy||this.referrerPolicy,S=n.transferCache??this.transferCache,C=n.timeout??this.timeout,$=n.body!==void 0?n.body:this.body,F=n.withCredentials??this.withCredentials,we=n.reportProgress??this.reportProgress,Je=n.reportUploadProgress??this.reportUploadProgress,Ls=n.reportDownloadProgress??this.reportDownloadProgress,wi=n.headers||this.headers,Vs=n.params||this.params,Bs=n.context??this.context;return n.setHeaders!==void 0&&(wi=Object.keys(n.setHeaders).reduce((So,xi)=>So.set(xi,n.setHeaders[xi]),wi)),n.setParams&&(Vs=Object.keys(n.setParams).reduce((So,xi)=>So.set(xi,n.setParams[xi]),Vs)),new t(e,i,$,{params:Vs,headers:wi,context:Bs,reportProgress:we,reportUploadProgress:Je,reportDownloadProgress:Ls,responseType:r,withCredentials:F,transferCache:S,keepalive:o,cache:a,priority:s,timeout:C,mode:l,redirect:c,credentials:u,referrer:f,integrity:h,referrerPolicy:p})}},Kr=(function(t){return t[t.Sent=0]="Sent",t[t.UploadProgress=1]="UploadProgress",t[t.ResponseHeader=2]="ResponseHeader",t[t.DownloadProgress=3]="DownloadProgress",t[t.Response=4]="Response",t[t.User=5]="User",t})(Kr||{}),Ha=class{headers;status;statusText;url;ok;type;redirected;responseType;constructor(n,e=200,i="OK"){this.headers=n.headers||new hi,this.status=n.status!==void 0?n.status:e,this.statusText=n.statusText||i,this.url=n.url||null,this.redirected=n.redirected,this.responseType=n.responseType,this.ok=this.status>=200&&this.status<300}},og=class t extends Ha{constructor(n={}){super(n)}type=Kr.ResponseHeader;clone(n={}){return new t({headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0})}},Ua=class t extends Ha{body;constructor(n={}){super(n),this.body=n.body!==void 0?n.body:null}type=Kr.Response;clone(n={}){return new t({body:n.body!==void 0?n.body:this.body,headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0,redirected:n.redirected??this.redirected,responseType:n.responseType??this.responseType})}},Xr=class extends Ha{name="HttpErrorResponse";message;error;ok=!1;constructor(n){super(n,0,"Unknown Error"),this.status>=200&&this.status<300?this.message=`Http failure during parsing for ${n.url||"(unknown url)"}`:this.message=`Http failure response for ${n.url||"(unknown url)"}: ${n.status} ${n.statusText}`,this.error=n.error||null}},kA=200;var AA=/^\)\]\}',?\n/,eY=1024*1024,RA=new b("",{factory:()=>null}),OA=(()=>{class t{fetchImpl=d(sg,{optional:!0})?.fetch??((...e)=>globalThis.fetch(...e));ngZone=d(N);destroyRef=d(We);maxResponseSize=d(RA);handle(e){return new ne(i=>{let r=new AbortController,o=!1,s={next:l=>{l.type===Kr.Response&&(o=!0),i.next(l)},error:l=>{o=!0,i.error(l)},complete:()=>{o=!0,i.complete()}};this.doRequest(e,r.signal,s).then(ag,l=>s.error(new Xr({error:l})));let a;return e.timeout&&(a=this.ngZone.runOutsideAngular(()=>setTimeout(()=>{r.signal.aborted||r.abort(new DOMException("signal timed out","TimeoutError"))},e.timeout))),()=>{a!==void 0&&clearTimeout(a),!o&&!r.signal.aborted&&r.abort()}})}async doRequest(e,i,r){let o=this.createRequestInit(e),s;try{let $=this.ngZone.runOutsideAngular(()=>this.fetchImpl(e.urlWithParams,v({signal:i},o)));PA($),r.next({type:Kr.Sent}),s=await $}catch($){r.error(new Xr({error:$,status:$.status??0,statusText:$.statusText,url:e.urlWithParams,headers:$.headers}));return}let a=new hi(s.headers),l=s.statusText,c=s.url||e.urlWithParams,u=s.status,f=null,h=e.reportProgress||e.reportDownloadProgress;if(h&&r.next(new og({headers:a,status:u,statusText:l,url:c})),s.body){let $=s.headers.get(tg)??"",F=s.headers.get("content-length"),we=F!==null?Number(F):NaN;this.maxResponseSize!==null&&Number.isFinite(we)&&we>this.maxResponseSize&&(await s.body.cancel(),H0(this.maxResponseSize));let Je=[],Ls=s.body.getReader(),wi=0,Vs,Bs,So=typeof Zone<"u"&&Zone.current,xi=!1;if(await this.ngZone.runOutsideAngular(async()=>{for(;;){if(this.destroyRef.destroyed){await Ls.cancel(),xi=!0;break}let{done:jf,value:Hf}=await Ls.read();if(jf)break;if(Je.push(Hf),wi+=Hf.length,this.maxResponseSize!==null&&wi>this.maxResponseSize&&(await Ls.cancel(),H0(this.maxResponseSize)),h){Bs=e.responseType==="text"?(Bs??"")+(Vs??=U0($)).decode(Hf,{stream:!0}):void 0;let zv=()=>r.next({type:Kr.DownloadProgress,total:Number.isFinite(we)?we:void 0,loaded:wi,partialText:Bs});So?So.run(zv):zv()}}}),xi){r.complete();return}let $w=this.concatChunks(Je,wi);try{f=this.parseBody(e,$w,$,u)}catch(jf){r.error(new Xr({error:jf,headers:new hi(s.headers),status:s.status,statusText:s.statusText,url:s.url||e.urlWithParams}));return}}u===0&&(u=f?kA:0);let p=u>=200&&u<300,S=s.redirected,C=s.type;p?(r.next(new Ua({body:f,headers:a,status:u,statusText:l,url:c,redirected:S,responseType:C})),r.complete()):r.error(new Xr({error:f,headers:a,status:u,statusText:l,url:c,redirected:S,responseType:C}))}parseBody(e,i,r,o){switch(e.responseType){case"json":let s=new TextDecoder().decode(i).replace(AA,"");if(s==="")return null;try{return JSON.parse(s)}catch(a){if(o<200||o>=300)return s;throw a}case"text":return U0(r).decode(i);case"blob":return new Blob([i],{type:r});case"arraybuffer":return i.buffer}}createRequestInit(e){if(e.reportUploadProgress)throw new x(2824,!1);let i={},r;if(r=e.credentials,e.withCredentials&&(r="include"),e.headers.forEach((o,s)=>i[o]=s.join(",")),e.headers.has(j0)||(i[j0]=NA),!e.headers.has(tg)){let o=e.detectContentTypeHeader();o!==null&&(i[tg]=o)}return{body:e.serializeBody(),method:e.method,headers:i,credentials:r,keepalive:e.keepalive,cache:e.cache,priority:e.priority,mode:e.mode,redirect:e.redirect,referrer:e.referrer,integrity:e.integrity,referrerPolicy:e.referrerPolicy}}concatChunks(e,i){let r=new Uint8Array(i),o=0;for(let s of e)r.set(s,o),o+=s.length;return r}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})(),sg=class{};function ag(){}function PA(t){t.then(ag,ag)}function H0(t){throw new x(-2825,!1)}var FA=/charset=\s*["']?([^;"'\s]+)["']?/i;function U0(t){let n=t.match(FA);if(n!==null)try{return new TextDecoder(n[1])}catch{}return new TextDecoder}var LA=new b("",{factory:()=>!0}),VA="XSRF-TOKEN",BA=new b("",{factory:()=>VA}),jA="X-XSRF-TOKEN",HA=new b("",{factory:()=>jA}),UA=(()=>{class t{cookieName=d(BA);doc=d(L);lastCookieString="";lastToken=null;parseCount=0;getToken(){let e=this.doc.cookie||"";return e!==this.lastCookieString&&(this.parseCount++,this.lastToken=Fa(e,this.cookieName),this.lastCookieString=e),this.lastToken}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})(),zA=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=G({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=A(UA),r},providedIn:"root"})}return t})();function $A(t,n){if(!d(LA)||t.method==="GET"||t.method==="HEAD")return n(t);try{let r=d(Qo).href,{origin:o}=new URL(r),{origin:s}=new URL(t.url,o);if(o!==s)return n(t)}catch{return n(t)}let e=d(zA).getToken(),i=d(HA);return e!=null&&!t.headers.has(i)&&(t=t.clone({headers:t.headers.set(i,e)})),n(t)}function GA(t,n){return n(t)}function WA(t,n,e){return(i,r)=>ot(e,()=>n(i,o=>t(o,r)))}var qA=new b("",{factory:()=>[$A]}),G0=new b(""),YA=new b("",{factory:()=>!0});var ZA=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=G({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=A(OA),r},providedIn:"root"})}return t})();var XA=(()=>{class t{backend;injector;chain=null;pendingTasks=d(ld);contributeToStability=d(YA);constructor(e,i){this.backend=e,this.injector=i}handle(e){if(this.chain===null){let r=this.injector.get(W0,null,{skipSelf:!0}),o=r!==null&&this.backend===r,s=this.injector.get(G0,[],o?{self:!0}:void 0),a=Array.from(new Set([...this.injector.get(qA),...s]));this.chain=a.reduceRight((l,c)=>WA(l,c,this.injector),GA)}let i=this.chain;if(this.contributeToStability){let r=this.pendingTasks.add();return Ne(()=>i(e,o=>this.backend.handle(o))).pipe(xr(r))}else return Ne(()=>i(e,r=>this.backend.handle(r)))}static \u0275fac=function(i){return new(i||t)(A(ZA),A(Ie))};static \u0275prov=G({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),W0=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=G({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=A(XA),r},providedIn:"root"})}return t})();function ng(t,n){return v({body:n},t)}var lg=(()=>{class t{handler;constructor(e){this.handler=e}request(e,i,r={}){let o;if(e instanceof Jo)o=e;else{let l;r.headers instanceof hi?l=r.headers:l=new hi(r.headers);let c;r.params&&(r.params instanceof fi?c=r.params:c=new fi({fromObject:r.params})),o=new Jo(e,i,r.body!==void 0?r.body:null,{headers:l,context:r.context,params:c,reportProgress:r.reportProgress,reportUploadProgress:r.reportUploadProgress,reportDownloadProgress:r.reportDownloadProgress,responseType:r.responseType||"json",withCredentials:r.withCredentials,transferCache:r.transferCache,keepalive:r.keepalive,priority:r.priority,cache:r.cache,mode:r.mode,redirect:r.redirect,credentials:r.credentials,referrer:r.referrer,referrerPolicy:r.referrerPolicy,integrity:r.integrity,timeout:r.timeout})}let s=Y(o).pipe(No(l=>this.handler.handle(l)));if(e instanceof Jo||r.observe==="events")return s;let a=s.pipe(ge(l=>l instanceof Ua));switch(r.observe||"body"){case"body":switch(o.responseType){case"arraybuffer":return a.pipe(oe(l=>{if(l.body!==null&&!(l.body instanceof ArrayBuffer))throw new x(2806,!1);return l.body}));case"blob":return a.pipe(oe(l=>{if(l.body!==null&&!(l.body instanceof Blob))throw new x(2807,!1);return l.body}));case"text":return a.pipe(oe(l=>{if(l.body!==null&&typeof l.body!="string")throw new x(2808,!1);return l.body}));default:return a.pipe(oe(l=>l.body))}case"response":return a;default:throw new x(2809,!1)}}delete(e,i={}){return this.request("DELETE",e,i)}get(e,i={}){return this.request("GET",e,i)}head(e,i={}){return this.request("HEAD",e,i)}jsonp(e,i){return this.request("JSONP",e,{params:new fi().append(i,"JSONP_CALLBACK"),observe:"body",responseType:"json"})}options(e,i={}){return this.request("OPTIONS",e,i)}patch(e,i,r={}){return this.request("PATCH",e,ng(r,i))}post(e,i,r={}){return this.request("POST",e,ng(r,i))}put(e,i,r={}){return this.request("PUT",e,ng(r,i))}static \u0275fac=function(i){return new(i||t)(A(W0))};static \u0275prov=G({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var q0=(()=>{class t{_doc;constructor(e){this._doc=e}getTitle(){return this._doc.title}setTitle(e){this._doc.title=e||""}static \u0275fac=function(i){return new(i||t)(A(L))};static \u0275prov=G({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var za=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=G({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=A(QA),r},providedIn:"root"})}return t})(),QA=(()=>{class t extends za{_doc=d(L);sanitize(e,i){if(i==null)return null;switch(e){case st.NONE:return i;case st.HTML:return Hi(i,"HTML")?vn(i):dp(this._doc,String(i)).toString();case st.STYLE:return Hi(i,"Style")?vn(i):i;case st.SCRIPT:if(Hi(i,"Script"))return vn(i);throw new x(5200,!1);case st.URL:return Hi(i,"URL")?vn(i):Ia(String(i));case st.RESOURCE_URL:if(Hi(i,"ResourceURL"))return vn(i);throw new x(-5201,!1);default:throw new x(5202,!1)}}bypassSecurityTrustHtml(e){return op(e)}bypassSecurityTrustStyle(e){return sp(e)}bypassSecurityTrustScript(e){return ap(e)}bypassSecurityTrustUrl(e){return lp(e)}bypassSecurityTrustResourceUrl(e){return cp(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();var se="primary",il=Symbol("RouteTitle"),hg=class{params;constructor(n){this.params=n||{}}has(n){return Object.hasOwn(this.params,n)}get(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e[0]:e}return null}getAll(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e:[e]}return[]}get keys(){return Object.keys(this.params)}};function Jr(t){return new hg(t)}function cg(t,n,e){for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(r[0]===":")e[r.substring(1)]=o;else if(r!==o.path)return!1}return!0}function nC(t,n,e){let i=e.path.split("/"),r=i.indexOf("**");if(r===-1){if(i.length>t.length||e.pathMatch==="full"&&(n.hasChildren()||i.length<t.length))return null;let l={},c=t.slice(0,i.length);return cg(i,c,l)?{consumed:c,posParams:l}:null}if(r!==i.lastIndexOf("**"))return null;let o=i.slice(0,r),s=i.slice(r+1);if(o.length+s.length>t.length||e.pathMatch==="full"&&n.hasChildren()&&e.path!=="**")return null;let a={};return!cg(o,t.slice(0,o.length),a)||!cg(s,t.slice(t.length-s.length),a)?null:{consumed:t,posParams:a}}function lu(t){return new Promise((n,e)=>{t.pipe(Kn()).subscribe({next:i=>n(i),error:i=>e(i)})})}function eR(t,n){if(t.length!==n.length)return!1;for(let e=0;e<t.length;++e)if(!$n(t[e],n[e]))return!1;return!0}function $n(t,n){let e=t?mg(t):void 0,i=n?mg(n):void 0;if(!e||!i||e.length!=i.length)return!1;let r;for(let o=0;o<e.length;o++)if(r=e[o],!iC(t[r],n[r]))return!1;return!0}function mg(t){return[...Object.keys(t),...Object.getOwnPropertySymbols(t)]}function iC(t,n){if(Array.isArray(t)&&Array.isArray(n)){if(t.length!==n.length)return!1;let e=[...t].sort(),i=[...n].sort();return e.every((r,o)=>i[o]===r)}else return t===n}function tR(t){return t.length>0?t[t.length-1]:null}function io(t){return qs(t)?t:di(t)?Ve(Promise.resolve(t)):Y(t)}function rC(t){return qs(t)?lu(t):Promise.resolve(t)}var nR={exact:aC,subset:lC},oC={exact:iR,subset:rR,ignored:()=>!0},sC={paths:"exact",fragment:"ignored",matrixParams:"ignored",queryParams:"exact"},pg={paths:"subset",fragment:"ignored",matrixParams:"ignored",queryParams:"subset"};function Z0(t,n,e){return nR[e.paths](t.root,n.root,e.matrixParams)&&oC[e.queryParams](t.queryParams,n.queryParams)&&!(e.fragment==="exact"&&t.fragment!==n.fragment)}function iR(t,n){return $n(t,n)}function aC(t,n,e){if(!Qr(t.segments,n.segments)||!ou(t.segments,n.segments,e)||t.numberOfChildren!==n.numberOfChildren)return!1;for(let i in n.children)if(!t.children[i]||!aC(t.children[i],n.children[i],e))return!1;return!0}function rR(t,n){return Object.keys(n).length<=Object.keys(t).length&&Object.keys(n).every(e=>iC(t[e],n[e]))}function lC(t,n,e){return cC(t,n,n.segments,e)}function cC(t,n,e,i){if(t.segments.length>e.length){let r=t.segments.slice(0,e.length);return!(!Qr(r,e)||n.hasChildren()||!ou(r,e,i))}else if(t.segments.length===e.length){if(!Qr(t.segments,e)||!ou(t.segments,e,i))return!1;for(let r in n.children)if(!t.children[r]||!lC(t.children[r],n.children[r],i))return!1;return!0}else{let r=e.slice(0,t.segments.length),o=e.slice(t.segments.length);return!Qr(t.segments,r)||!ou(t.segments,r,i)||!t.children[se]?!1:cC(t.children[se],n,o,i)}}function ou(t,n,e){return n.every((i,r)=>oC[e](t[r].parameters,i.parameters))}var nn=class{root;queryParams;fragment;_queryParamMap;constructor(n=new Se([],{}),e={},i=null){this.root=n,this.queryParams=e,this.fragment=i}get queryParamMap(){return this._queryParamMap??=Jr(this.queryParams),this._queryParamMap}toString(){return aR.serialize(this)}},Se=class{segments;children;parent=null;constructor(n,e){this.segments=n,this.children=e,Object.values(e).forEach(i=>i.parent=this)}hasChildren(){return this.numberOfChildren>0}get numberOfChildren(){return Object.keys(this.children).length}toString(){return su(this)}},Yi=class{path;parameters;_parameterMap;constructor(n,e){this.path=n,this.parameters=e}get parameterMap(){return this._parameterMap??=Jr(this.parameters),this._parameterMap}toString(){return uC(this)}};function oR(t,n){return Qr(t,n)&&t.every((e,i)=>$n(e.parameters,n[i].parameters))}function Qr(t,n){return t.length!==n.length?!1:t.every((e,i)=>e.path===n[i].path)}function sR(t,n){let e=[];return Object.entries(t.children).forEach(([i,r])=>{i===se&&(e=e.concat(n(r,i)))}),Object.entries(t.children).forEach(([i,r])=>{i!==se&&(e=e.concat(n(r,i)))}),e}var rl=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:()=>new Zi})}return t})(),Zi=class{parse(n){let e=new vg(n);return new nn(e.parseRootSegment(),e.parseQueryParams(),e.parseFragment())}serialize(n){let e=`/${$a(n.root,!0)}`,i=dR(n.queryParams),r=typeof n.fragment=="string"?`#${lR(n.fragment)}`:"";return`${e}${i}${r}`}},aR=new Zi;function su(t){return t.segments.map(n=>uC(n)).join("/")}function $a(t,n){if(!t.hasChildren())return su(t);if(n){let e=t.children[se]?$a(t.children[se],!1):"",i=[];return Object.entries(t.children).forEach(([r,o])=>{r!==se&&i.push(`${r}:${$a(o,!1)}`)}),i.length>0?`${e}(${i.join("//")})`:e}else{let e=sR(t,(i,r)=>r===se?[$a(t.children[se],!1)]:[`${r}:${$a(i,!1)}`]);return Object.keys(t.children).length===1&&t.children[se]!=null?`${su(t)}/${e[0]}`:`${su(t)}/(${e.join("//")})`}}function dC(t){return encodeURIComponent(t).replace(/%40/g,"@").replace(/%3A/gi,":").replace(/%24/g,"$").replace(/%2C/gi,",")}function iu(t){return dC(t).replace(/%3B/gi,";")}function lR(t){return encodeURI(t)}function gg(t){return dC(t).replace(/\(/g,"%28").replace(/\)/g,"%29").replace(/%26/gi,"&")}function au(t){return decodeURIComponent(t)}function X0(t){return au(t.replace(/\+/g,"%20"))}function uC(t){return`${gg(t.path)}${cR(t.parameters)}`}function cR(t){return Object.entries(t).map(([n,e])=>`;${gg(n)}=${gg(e)}`).join("")}function dR(t){let n=Object.entries(t).map(([e,i])=>Array.isArray(i)?i.map(r=>`${iu(e)}=${iu(r)}`).join("&"):`${iu(e)}=${iu(i)}`).filter(e=>e);return n.length?`?${n.join("&")}`:""}var uR=/^[^\/()?;#]+/;function dg(t){let n=t.match(uR);return n?n[0]:""}var fR=/^[^\/()?;=#]+/;function hR(t){let n=t.match(fR);return n?n[0]:""}var mR=/^[^=?&#]+/;function pR(t){let n=t.match(mR);return n?n[0]:""}var gR=/^[^&#]+/;function vR(t){let n=t.match(gR);return n?n[0]:""}var vg=class{url;remaining;constructor(n){this.url=n,this.remaining=n}parseRootSegment(){for(;this.consumeOptional("/"););return this.remaining===""||this.peekStartsWith("?")||this.peekStartsWith("#")?new Se([],{}):new Se([],this.parseChildren())}parseQueryParams(){let n={};if(this.consumeOptional("?"))do this.parseQueryParam(n);while(this.consumeOptional("&"));return n}parseFragment(){return this.consumeOptional("#")?decodeURIComponent(this.remaining):null}parseChildren(n=0){if(n>50)throw new x(4010,!1);if(this.remaining==="")return{};this.consumeOptional("/");let e=[];for(this.peekStartsWith("(")||e.push(this.parseSegment());this.peekStartsWith("/")&&!this.peekStartsWith("//")&&!this.peekStartsWith("/(");)this.capture("/"),e.push(this.parseSegment());let i={};this.peekStartsWith("/(")&&(this.capture("/"),i=this.parseParens(!0,n));let r={};return this.peekStartsWith("(")&&(r=this.parseParens(!1,n)),(e.length>0||Object.keys(i).length>0)&&(r[se]=new Se(e,i)),r}parseSegment(){let n=dg(this.remaining);if(n===""&&this.peekStartsWith(";"))throw new x(4009,!1);return this.capture(n),new Yi(au(n),this.parseMatrixParams())}parseMatrixParams(){let n={};for(;this.consumeOptional(";");)this.parseParam(n);return n}parseParam(n){let e=hR(this.remaining);if(!e)return;this.capture(e);let i="";if(this.consumeOptional("=")){let r=dg(this.remaining);r&&(i=r,this.capture(i))}n[au(e)]=au(i)}parseQueryParam(n){let e=pR(this.remaining);if(!e)return;this.capture(e);let i="";if(this.consumeOptional("=")){let s=vR(this.remaining);s&&(i=s,this.capture(i))}let r=X0(e),o=X0(i);if(Object.hasOwn(n,r)){let s=n[r];Array.isArray(s)||(s=[s],n[r]=s),s.push(o)}else n[r]=o}parseParens(n,e){let i=Object.create(null);for(this.capture("(");!this.consumeOptional(")")&&this.remaining.length>0;){let r=dg(this.remaining),o=this.remaining[r.length];if(o!=="/"&&o!==")"&&o!==";")throw new x(4010,!1);let s;r.indexOf(":")>-1?(s=r.slice(0,r.indexOf(":")),this.capture(s),this.capture(":")):n&&(s=se);let a=this.parseChildren(e+1);i[s??se]=Object.keys(a).length===1&&a[se]?a[se]:new Se([],a),this.consumeOptional("//")}return i}peekStartsWith(n){return this.remaining.startsWith(n)}consumeOptional(n){return this.peekStartsWith(n)?(this.remaining=this.remaining.substring(n.length),!0):!1}capture(n){if(!this.consumeOptional(n))throw new x(4011,!1)}};function fC(t){return t.segments.length>0?new Se([],{[se]:t}):t}function hC(t){let n=Object.create(null);for(let[i,r]of Object.entries(t.children)){let o=hC(r);if(i===se&&o.segments.length===0&&o.hasChildren())for(let[s,a]of Object.entries(o.children))n[s]=a;else(o.segments.length>0||o.hasChildren())&&(n[i]=o)}let e=new Se(t.segments,n);return bR(e)}function bR(t){if(t.numberOfChildren===1&&t.children[se]){let n=t.children[se];return new Se(t.segments.concat(n.segments),n.children)}return t}function is(t){return t instanceof nn}function mC(t,n,e=null,i=null,r=new Zi){let o=pC(t);return gC(o,n,e,i,r)}function pC(t){let n;function e(o){let s={};for(let l of o.children){let c=e(l);s[l.outlet]=c}let a=new Se(o.url,s);return o===t&&(n=a),a}let i=e(t.root),r=fC(i);return n??r}function gC(t,n,e,i,r){let o=t;for(;o.parent;)o=o.parent;if(n.length===0)return ug(o,o,o,e,i,r);let s=_R(n);if(s.toRoot())return ug(o,o,new Se([],{}),e,i,r);let a=yR(s,o,t),l=a.processChildren?Wa(a.segmentGroup,a.index,s.commands):bC(a.segmentGroup,a.index,s.commands);return ug(o,a.segmentGroup,l,e,i,r)}function cu(t){return typeof t=="object"&&t!=null&&!t.outlets&&!t.segmentPath}function Za(t){return typeof t=="object"&&t!=null&&t.outlets}function K0(t,n,e){t||="\u0275";let i=new nn;return i.queryParams={[t]:n},e.parse(e.serialize(i)).queryParams[t]}function ug(t,n,e,i,r,o){let s={};for(let[c,u]of Object.entries(i??{}))s[c]=Array.isArray(u)?u.map(f=>K0(c,f,o)):K0(c,u,o);let a;t===n?a=e:a=vC(t,n,e);let l=fC(hC(a));return new nn(l,s,r)}function vC(t,n,e){let i=Object.create(null);return Object.entries(t.children).forEach(([r,o])=>{o===n?i[r]=e:i[r]=vC(o,n,e)}),new Se(t.segments,i)}var du=class{isAbsolute;numberOfDoubleDots;commands;constructor(n,e,i){if(this.isAbsolute=n,this.numberOfDoubleDots=e,this.commands=i,n&&i.length>0&&cu(i[0]))throw new x(4003,!1);let r=i.find(Za);if(r&&r!==tR(i))throw new x(4004,!1)}toRoot(){return this.isAbsolute&&this.commands.length===1&&this.commands[0]=="/"}};function _R(t){if(typeof t[0]=="string"&&t.length===1&&t[0]==="/")return new du(!0,0,t);let n=0,e=!1,i=t.reduce((r,o,s)=>{if(typeof o=="object"&&o!=null){if(o.outlets){let a={};return Object.entries(o.outlets).forEach(([l,c])=>{a[l]=typeof c=="string"?c.split("/"):c}),[...r,{outlets:a}]}if(o.segmentPath)return[...r,o.segmentPath]}return typeof o!="string"?[...r,o]:s===0?(o.split("/").forEach((a,l)=>{l==0&&a==="."||(l==0&&a===""?e=!0:a===".."?n++:a!=""&&r.push(a))}),r):[...r,o]},[]);return new du(e,n,i)}var ts=class{segmentGroup;processChildren;index;constructor(n,e,i){this.segmentGroup=n,this.processChildren=e,this.index=i}};function yR(t,n,e){if(t.isAbsolute)return new ts(n,!0,0);if(!e)return new ts(n,!1,NaN);if(e.parent===null)return new ts(e,!0,0);let i=cu(t.commands[0])?0:1,r=e.segments.length-1+i;return SR(e,r,t.numberOfDoubleDots)}function SR(t,n,e){let i=t,r=n,o=e;for(;o>r;){if(o-=r,i=i.parent,!i)throw new x(4005,!1);r=i.segments.length}return new ts(i,!1,r-o)}function CR(t){return Za(t[0])?t[0].outlets:{[se]:t}}function bC(t,n,e){if(t??=new Se([],{}),t.segments.length===0&&t.hasChildren())return Wa(t,n,e);let i=DR(t,n,e),r=e.slice(i.commandIndex);if(i.match&&i.pathIndex<t.segments.length){let o=new Se(t.segments.slice(0,i.pathIndex),{});return o.children[se]=new Se(t.segments.slice(i.pathIndex),t.children),Wa(o,0,r)}else return i.match&&r.length===0?new Se(t.segments,{}):i.match&&!t.hasChildren()?bg(t,n,e):i.match?Wa(t,0,r):bg(t,n,e)}function Wa(t,n,e){if(e.length===0)return new Se(t.segments,{});{let i=CR(e),r=Object.create(null);if(Object.keys(i).some(o=>o!==se)&&t.children[se]&&t.numberOfChildren===1&&t.children[se].segments.length===0){let o=Wa(t.children[se],n,e);return new Se(t.segments,o.children)}return Object.entries(i).forEach(([o,s])=>{typeof s=="string"&&(s=[s]),s!==null&&(r[o]=bC(t.children[o],n,s))}),Object.entries(t.children).forEach(([o,s])=>{i[o]===void 0&&(r[o]=s)}),new Se(t.segments,r)}}function DR(t,n,e){let i=0,r=n,o={match:!1,pathIndex:0,commandIndex:0};for(;r<t.segments.length;){if(i>=e.length)return o;let s=t.segments[r],a=e[i];if(Za(a))break;let l=`${a}`,c=i<e.length-1?e[i+1]:null;if(r>0&&l===void 0)break;if(l&&c&&typeof c=="object"&&c.outlets===void 0){if(!J0(l,c,s))return o;i+=2}else{if(!J0(l,{},s))return o;i++}r++}return{match:!0,pathIndex:r,commandIndex:i}}function bg(t,n,e){let i=t.segments.slice(0,n),r=0;for(;r<e.length;){let o=e[r];if(Za(o)){let l=ER(o.outlets);return new Se(i,l)}if(r===0&&cu(e[0])){let l=t.segments[n];i.push(new Yi(l.path,Q0(e[0]))),r++;continue}let s=Za(o)?o.outlets[se]:`${o}`,a=r<e.length-1?e[r+1]:null;s&&a&&cu(a)?(i.push(new Yi(s,Q0(a))),r+=2):(i.push(new Yi(s,{})),r++)}return new Se(i,{})}function ER(t){let n={};return Object.entries(t).forEach(([e,i])=>{typeof i=="string"&&(i=[i]),i!==null&&(n[e]=bg(new Se([],{}),0,i))}),n}function Q0(t){let n={};return Object.entries(t).forEach(([e,i])=>n[e]=`${i}`),n}function J0(t,n,e){return t==e.path&&$n(n,e.parameters)}var qa="imperative",mt=(function(t){return t[t.NavigationStart=0]="NavigationStart",t[t.NavigationEnd=1]="NavigationEnd",t[t.NavigationCancel=2]="NavigationCancel",t[t.NavigationError=3]="NavigationError",t[t.RoutesRecognized=4]="RoutesRecognized",t[t.ResolveStart=5]="ResolveStart",t[t.ResolveEnd=6]="ResolveEnd",t[t.GuardsCheckStart=7]="GuardsCheckStart",t[t.GuardsCheckEnd=8]="GuardsCheckEnd",t[t.RouteConfigLoadStart=9]="RouteConfigLoadStart",t[t.RouteConfigLoadEnd=10]="RouteConfigLoadEnd",t[t.ChildActivationStart=11]="ChildActivationStart",t[t.ChildActivationEnd=12]="ChildActivationEnd",t[t.ActivationStart=13]="ActivationStart",t[t.ActivationEnd=14]="ActivationEnd",t[t.Scroll=15]="Scroll",t[t.NavigationSkipped=16]="NavigationSkipped",t})(mt||{}),zt=class{id;url;constructor(n,e){this.id=n,this.url=e}},eo=class extends zt{type=mt.NavigationStart;navigationTrigger;restoredState;constructor(n,e,i="imperative",r=null){super(n,e),this.navigationTrigger=i,this.restoredState=r}toString(){return`NavigationStart(id: ${this.id}, url: '${this.url}')`}},pi=class extends zt{urlAfterRedirects;type=mt.NavigationEnd;constructor(n,e,i){super(n,e),this.urlAfterRedirects=i}toString(){return`NavigationEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}')`}},xt=(function(t){return t[t.Redirect=0]="Redirect",t[t.SupersededByNewNavigation=1]="SupersededByNewNavigation",t[t.NoDataFromResolver=2]="NoDataFromResolver",t[t.GuardRejected=3]="GuardRejected",t[t.Aborted=4]="Aborted",t})(xt||{}),Xa=(function(t){return t[t.IgnoredSameUrlNavigation=0]="IgnoredSameUrlNavigation",t[t.IgnoredByUrlHandlingStrategy=1]="IgnoredByUrlHandlingStrategy",t})(Xa||{}),tn=class extends zt{reason;code;type=mt.NavigationCancel;constructor(n,e,i,r){super(n,e),this.reason=i,this.code=r}toString(){return`NavigationCancel(id: ${this.id}, url: '${this.url}')`}};function _C(t){return t instanceof tn&&(t.code===xt.Redirect||t.code===xt.SupersededByNewNavigation)}var gi=class extends zt{reason;code;type=mt.NavigationSkipped;constructor(n,e,i,r){super(n,e),this.reason=i,this.code=r}},to=class extends zt{error;target;type=mt.NavigationError;constructor(n,e,i,r){super(n,e),this.error=i,this.target=r}toString(){return`NavigationError(id: ${this.id}, url: '${this.url}', error: ${this.error})`}},Ka=class extends zt{urlAfterRedirects;state;type=mt.RoutesRecognized;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`RoutesRecognized(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},uu=class extends zt{urlAfterRedirects;state;type=mt.GuardsCheckStart;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`GuardsCheckStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},fu=class extends zt{urlAfterRedirects;state;shouldActivate;type=mt.GuardsCheckEnd;constructor(n,e,i,r,o){super(n,e),this.urlAfterRedirects=i,this.state=r,this.shouldActivate=o}toString(){return`GuardsCheckEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state}, shouldActivate: ${this.shouldActivate})`}},hu=class extends zt{urlAfterRedirects;state;type=mt.ResolveStart;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},mu=class extends zt{urlAfterRedirects;state;type=mt.ResolveEnd;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},pu=class{route;type=mt.RouteConfigLoadStart;constructor(n){this.route=n}toString(){return`RouteConfigLoadStart(path: ${this.route.path})`}},gu=class{route;type=mt.RouteConfigLoadEnd;constructor(n){this.route=n}toString(){return`RouteConfigLoadEnd(path: ${this.route.path})`}},vu=class{snapshot;type=mt.ChildActivationStart;constructor(n){this.snapshot=n}toString(){return`ChildActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},bu=class{snapshot;type=mt.ChildActivationEnd;constructor(n){this.snapshot=n}toString(){return`ChildActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},_u=class{snapshot;type=mt.ActivationStart;constructor(n){this.snapshot=n}toString(){return`ActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},yu=class{snapshot;type=mt.ActivationEnd;constructor(n){this.snapshot=n}toString(){return`ActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}};var rs=class{},Qa=class{},os=class{url;navigationBehaviorOptions;constructor(n,e){this.url=n,this.navigationBehaviorOptions=e}};function wR(t){return!(t instanceof rs)&&!(t instanceof os)&&!(t instanceof Qa)}var Su=class{rootInjector;outlet=null;route=null;children;attachRef=null;get injector(){return this.route?.snapshot._environmentInjector??this.rootInjector}constructor(n){this.rootInjector=n,this.children=new no(this.rootInjector)}resetChildren(){this.children=new no(this.rootInjector)}},no=(()=>{class t{rootInjector;contexts=new Map;constructor(e){this.rootInjector=e}onChildOutletCreated(e,i){let r=this.getOrCreateContext(e);r.outlet=i,this.contexts.set(e,r)}onChildOutletDestroyed(e){let i=this.getContext(e);i&&(i.outlet=null,i.attachRef=null)}onOutletDeactivated(){let e=this.contexts;return this.contexts=new Map,e}onOutletReAttached(e){this.contexts=e}getOrCreateContext(e){let i=this.getContext(e);return i||(i=new Su(this.rootInjector),this.contexts.set(e,i)),i}getContext(e){return this.contexts.get(e)||null}static \u0275fac=function(i){return new(i||t)(A(Ie))};static \u0275prov=G({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Cu=class{_root;constructor(n){this._root=n}get root(){return this._root.value}parent(n){let e=this.pathFromRoot(n);return e.length>1?e[e.length-2]:null}children(n){let e=_g(n,this._root);return e?e.children.map(i=>i.value):[]}firstChild(n){let e=_g(n,this._root);return e&&e.children.length>0?e.children[0].value:null}siblings(n){let e=yg(n,this._root);return e.length<2?[]:e[e.length-2].children.map(r=>r.value).filter(r=>r!==n)}pathFromRoot(n){return yg(n,this._root).map(e=>e.value)}};function _g(t,n){if(t===n.value)return n;for(let e of n.children){let i=_g(t,e);if(i)return i}return null}function yg(t,n){if(t===n.value)return[n];for(let e of n.children){let i=yg(t,e);if(i.length)return i.unshift(n),i}return[]}var Ut=class{value;children;constructor(n,e){this.value=n,this.children=e}toString(){return`TreeNode(${this.value})`}};function es(t){let n={};return t&&t.children.forEach(e=>n[e.value.outlet]=e),n}var Ja=class extends Cu{snapshot;constructor(n,e){super(n),this.snapshot=e,Ng(this,n)}toString(){return this.snapshot.toString()}};function yC(t,n){let e=xR(t,n),i=new tt([new Yi("",{})]),r=new tt({}),o=new tt({}),s=new tt({}),a=new tt(""),l=new Xi(i,r,s,a,o,se,t,e.root);return l.snapshot=e.root,new Ja(new Ut(l,[]),e)}function xR(t,n){let e={},i={},r={},s=new ss([],e,r,"",i,se,t,null,{},n);return new el("",new Ut(s,[]))}var Xi=class{urlSubject;paramsSubject;queryParamsSubject;fragmentSubject;dataSubject;outlet;component;snapshot;_futureSnapshot;_routerState;_paramMap;_queryParamMap;title;url;params;queryParams;fragment;data;resources;_localInjector;pending;paramsSignal;queryParamsSignal;paramMapSignal;queryParamMapSignal;fragmentSignal;dataSignal;constructor(n,e,i,r,o,s,a,l){this.urlSubject=n,this.paramsSubject=e,this.queryParamsSubject=i,this.fragmentSubject=r,this.dataSubject=o,this.outlet=s,this.component=a,this._futureSnapshot=l,this.title=this.dataSubject?.pipe(oe(c=>c[il]))??Y(void 0),this.url=n,this.params=e,this.queryParams=i,this.fragment=r,this.data=o}get routeConfig(){return this._futureSnapshot.routeConfig}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=this.params.pipe(oe(n=>Jr(n))),this._paramMap}get queryParamMap(){return this._queryParamMap??=this.queryParams.pipe(oe(n=>Jr(n))),this._queryParamMap}toString(){return this.snapshot?this.snapshot.toString():`Future(${this._futureSnapshot})`}_setPending(n){this._futureSnapshot=n,this.pending?.set(!0)}},IR="always";function Tg(t,n,e){let i,{routeConfig:r}=t;return n!==null&&(e==="always"||r?.path===""||!n.component&&!n.routeConfig?.loadComponent)?i={params:v(v({},n.params),t.params),data:v(v({},n.data),t.data),resolve:v(v(v(v({},t.data),n.data),r?.data),t._resolvedData)}:i={params:v({},t.params),data:v({},t.data),resolve:v(v({},t.data),t._resolvedData??{})},r&&CC(r)&&(i.resolve[il]=r.title),i}var ss=class{url;params;queryParams;fragment;data;outlet;component;routeConfig;_resolve;_resolvedData;_routerState;_paramMap;_queryParamMap;_environmentInjector;resources;get title(){return this.data?.[il]}constructor(n,e,i,r,o,s,a,l,c,u){this.url=n,this.params=e,this.queryParams=i,this.fragment=r,this.data=o,this.outlet=s,this.component=a,this.routeConfig=l,this._resolve=c,this._environmentInjector=u}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=Jr(this.params),this._paramMap}get queryParamMap(){return this._queryParamMap??=Jr(this.queryParams),this._queryParamMap}toString(){let n=this.url.map(i=>i.toString()).join("/"),e=this.routeConfig?this.routeConfig.path:"";return`Route(url:'${n}', path:'${e}')`}},el=class extends Cu{url;constructor(n,e){super(e),this.url=n,Ng(this,e)}toString(){return SC(this._root)}};function Ng(t,n){n.value._routerState=t,n.children.forEach(e=>Ng(t,e))}function SC(t){let n=t.children.length>0?` { ${t.children.map(SC).join(", ")} } `:"";return`${t.value}${n}`}function fg(t){if(t.snapshot){let n=t.snapshot,e=t._futureSnapshot;t.snapshot=e,$n(n.queryParams,e.queryParams)||t.queryParamsSubject.next(e.queryParams),n.fragment!==e.fragment&&t.fragmentSubject.next(e.fragment),$n(n.params,e.params)||t.paramsSubject.next(e.params),eR(n.url,e.url)||t.urlSubject.next(e.url),$n(n.data,e.data)||t.dataSubject.next(e.data)}else t.snapshot=t._futureSnapshot,t.dataSubject.next(t._futureSnapshot.data)}function Sg(t,n){let e=$n(t.params,n.params)&&oR(t.url,n.url),i=!t.parent!=!n.parent;return e&&!i&&(!t.parent||Sg(t.parent,n.parent))}function CC(t){return typeof t.title=="string"||t.title===null}var DC=new b(""),ol=(()=>{class t{activated=null;get activatedComponentRef(){return this.activated}_activatedRoute=null;name=se;activateEvents=new W;deactivateEvents=new W;attachEvents=new W;detachEvents=new W;routerOutletData=yn();parentContexts=d(no);location=d(bt);changeDetector=d(Oe);inputBinder=d(xu,{optional:!0});supportsBindingToComponentInputs=!0;ngOnChanges(e){if(e.name){let{firstChange:i,previousValue:r}=e.name;if(i)return;this.isTrackedInParentContexts(r)&&(this.deactivate(),this.parentContexts.onChildOutletDestroyed(r)),this.initializeOutletWithName()}}ngOnDestroy(){this.isTrackedInParentContexts(this.name)&&this.parentContexts.onChildOutletDestroyed(this.name),this.inputBinder?.unsubscribeFromRouteData(this)}isTrackedInParentContexts(e){return this.parentContexts.getContext(e)?.outlet===this}ngOnInit(){this.initializeOutletWithName()}initializeOutletWithName(){if(this.parentContexts.onChildOutletCreated(this.name,this),this.activated)return;let e=this.parentContexts.getContext(this.name);e?.route&&(e.attachRef?this.attach(e.attachRef,e.route):this.activateWith(e.route,e.injector))}get isActivated(){return!!this.activated}get component(){if(!this.activated)throw new x(4012,!1);return this.activated.instance}get activatedRoute(){if(!this.activated)throw new x(4012,!1);return this._activatedRoute}get activatedRouteData(){return this._activatedRoute?this._activatedRoute.snapshot.data:{}}detach(){if(!this.activated)throw new x(4012,!1);this.location.detach();let e=this.activated;return this.activated=null,this._activatedRoute=null,this.detachEvents.emit(e.instance),e}attach(e,i){this.activated=e,this._activatedRoute=i,this.location.insert(e.hostView),this.inputBinder?.bindActivatedRouteToOutletComponent(this,this.location.injector),this.attachEvents.emit(e.instance)}deactivate(){if(this.activated){let e=this.component;this.activated.destroy(),this.activated=null,this._activatedRoute=null,this.deactivateEvents.emit(e)}}activateWith(e,i){if(this.isActivated)throw new x(4013,!1);this._activatedRoute=e;let r=this.location,s=e.snapshot.component,a=this.parentContexts.getOrCreateContext(this.name).children,l=new Cg(e,a,r.injector,this.routerOutletData);this.activated=r.createComponent(s,{index:r.length,injector:l,environmentInjector:i}),this.changeDetector.markForCheck(),this.inputBinder?.bindActivatedRouteToOutletComponent(this,this.location.injector),this.activateEvents.emit(this.activated.instance)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,selectors:[["router-outlet"]],inputs:{name:"name",routerOutletData:[1,"routerOutletData"]},outputs:{activateEvents:"activate",deactivateEvents:"deactivate",attachEvents:"attach",detachEvents:"detach"},exportAs:["outlet"],features:[at]})}return t})(),Cg=class{route;childContexts;parent;outletData;constructor(n,e,i,r){this.route=n,this.childContexts=e,this.parent=i,this.outletData=r}get(n,e){return n===Xi?this.route:n===no?this.childContexts:n===DC?this.outletData:this.parent.get(n,e)}},xu=new b("");var kg=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["ng-component"]],exportAs:["emptyRouterOutlet"],decls:1,vars:0,template:function(i,r){i&1&&ie(0,"router-outlet")},dependencies:[ol],encapsulation:2,changeDetection:1})}return t})();function Ag(t){let n=t.children&&t.children.map(Ag),e=n?k(v({},t),{children:n}):v({},t);return!e.component&&!e.loadComponent&&(n||e.loadChildren)&&e.outlet&&e.outlet!==se&&(e.component=kg),e}function MR(t,n,e){let i=new Set,r=tl(t,n._root,e?e._root:void 0,i);return{newlyCreatedRoutes:i,state:new Ja(r,n)}}function tl(t,n,e,i){if(e&&t.shouldReuseRoute(n.value,e.value.snapshot)){let r=e.value;r._setPending(n.value);let o=TR(t,n,e,i);return new Ut(r,o)}else{if(t.shouldAttach(n.value)){let s=t.retrieve(n.value);if(s!==null){let a=s.route;return a.value._setPending(n.value),a.children=n.children.map(l=>tl(t,l,void 0,i)),a}}let r=NR(n.value);r._setPending(n.value),i.add(r);let o=n.children.map(s=>tl(t,s,void 0,i));return new Ut(r,o)}}function TR(t,n,e,i){return n.children.map(r=>{for(let o of e.children)if(t.shouldReuseRoute(r.value,o.value.snapshot))return tl(t,r,o,i);return tl(t,r,void 0,i)})}function NR(t){return new Xi(new tt(t.url),new tt(t.params),new tt(t.queryParams),new tt(t.fragment),new tt(t.data),t.outlet,t.component,t)}var as=class{redirectTo;navigationBehaviorOptions;constructor(n,e){this.redirectTo=n,this.navigationBehaviorOptions=e}},EC="ngNavigationCancelingError";function Du(t,n){let{redirectTo:e,navigationBehaviorOptions:i}=is(n)?{redirectTo:n,navigationBehaviorOptions:void 0}:n,r=wC(!1,xt.Redirect);return r.url=e,r.navigationBehaviorOptions=i,r}function wC(t,n){let e=new Error(`NavigationCancelingError: ${t||""}`);return e[EC]=!0,e.cancellationCode=n,e}function kR(t){return xC(t)&&is(t.url)}function xC(t){return!!t&&t[EC]}var Dg=class{routeReuseStrategy;futureState;currState;forwardEvent;inputBindingEnabled;constructor(n,e,i,r,o){this.routeReuseStrategy=n,this.futureState=e,this.currState=i,this.forwardEvent=r,this.inputBindingEnabled=o}activate(n){let e=this.futureState._root,i=this.currState?this.currState._root:null;this.deactivateChildRoutes(e,i,n),fg(this.futureState.root),this.activateChildRoutes(e,i,n)}deactivateChildRoutes(n,e,i){let r=es(e);n.children.forEach(o=>{let s=o.value.outlet;this.deactivateRoutes(o,r[s],i),delete r[s]}),Object.values(r).forEach(o=>{this.deactivateRouteAndItsChildren(o,i)})}deactivateRoutes(n,e,i){let r=n.value,o=e?e.value:null;if(r===o)if(r.component){let s=i.getContext(r.outlet);s&&this.deactivateChildRoutes(n,e,s.children)}else this.deactivateChildRoutes(n,e,i);else o&&this.deactivateRouteAndItsChildren(e,i)}deactivateRouteAndItsChildren(n,e){n.value.component&&this.routeReuseStrategy.shouldDetach(n.value.snapshot)?this.detachAndStoreRouteSubtree(n,e):this.deactivateRouteAndOutlet(n,e)}detachAndStoreRouteSubtree(n,e){let i=e.getContext(n.value.outlet),r=i&&n.value.component?i.children:e,o=es(n);for(let s of Object.values(o))this.deactivateRouteAndItsChildren(s,r);if(i&&i.outlet){let s=i.outlet.detach(),a=i.children.contexts;i.resetChildren(),this.routeReuseStrategy.store(n.value.snapshot,{componentRef:s,route:n,contexts:a})}}deactivateRouteAndOutlet(n,e){let i=e.getContext(n.value.outlet),r=i&&n.value.component?i.children:e,o=es(n);for(let s of Object.values(o))this.deactivateRouteAndItsChildren(s,r);i&&(i.outlet&&(i.outlet.deactivate(),i.children.onOutletDeactivated()),i.attachRef=null,i.route=null),n.value._localInjector?.destroy()}activateChildRoutes(n,e,i){let r=es(e);n.children.forEach(o=>{this.activateRoutes(o,r[o.value.outlet],i),this.forwardEvent(new yu(o.value.snapshot))}),n.children.length&&this.forwardEvent(new bu(n.value.snapshot))}activateRoutes(n,e,i){let r=n.value,o=e?e.value:null;if(fg(r),r===o)if(r.component){let s=i.getOrCreateContext(r.outlet);this.activateChildRoutes(n,e,s.children)}else this.activateChildRoutes(n,e,i);else if(r.component){let s=i.getOrCreateContext(r.outlet);if(this.routeReuseStrategy.shouldAttach(r.snapshot)){let a=this.routeReuseStrategy.retrieve(r.snapshot);this.routeReuseStrategy.store(r.snapshot,null),s.children.onOutletReAttached(a.contexts),s.attachRef=a.componentRef,s.route=a.route.value,s.outlet&&s.outlet.attach(a.componentRef,a.route.value),fg(a.route.value),this.activateChildRoutes(n,null,s.children)}else s.attachRef=null,s.route=r,s.outlet&&s.outlet.activateWith(r,s.injector),this.activateChildRoutes(n,null,s.children)}else this.activateChildRoutes(n,null,i)}},Eu=class{path;route;constructor(n){this.path=n,this.route=this.path[this.path.length-1]}},ns=class{component;route;constructor(n,e){this.component=n,this.route=e}};function AR(t,n,e){let i=t._root,r=n?n._root:null;return Ga(i,r,e,[i.value])}function RR(t){let n=t.routeConfig?t.routeConfig.canActivateChild:null;return!n||n.length===0?null:{node:t,guards:n}}function cs(t,n){let e=Symbol(),i=n.get(t,e);return i===e?typeof t=="function"&&!Sh(t)?t:n.get(t):i}function Ga(t,n,e,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=es(n);return t.children.forEach(s=>{OR(s,o[s.value.outlet],e,i.concat([s.value]),r),delete o[s.value.outlet]}),Object.entries(o).forEach(([s,a])=>Ya(a,e.getContext(s),e,r)),r}function OR(t,n,e,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=t.value,s=n?n.value:null,a=e?e.getContext(t.value.outlet):null;if(s&&o.routeConfig===s.routeConfig){let l=PR(s,o,o.routeConfig.runGuardsAndResolvers);l?r.canActivateChecks.push(new Eu(i)):(o.data=s.data,o._resolvedData=s._resolvedData),o.component?Ga(t,n,a?a.children:null,i,r):Ga(t,n,e,i,r),l&&a&&a.outlet&&a.outlet.isActivated&&r.canDeactivateChecks.push(new ns(a.outlet.component,s))}else s&&Ya(n,a,e,r),r.canActivateChecks.push(new Eu(i)),o.component?Ga(t,null,a?a.children:null,i,r):Ga(t,null,e,i,r);return r}function PR(t,n,e){if(typeof e=="function")return ot(n._environmentInjector,()=>e(t,n));switch(e){case"pathParamsChange":return!Qr(t.url,n.url);case"pathParamsOrQueryParamsChange":return!Qr(t.url,n.url)||!$n(t.queryParams,n.queryParams);case"always":return!0;case"paramsOrQueryParamsChange":return!Sg(t,n)||!$n(t.queryParams,n.queryParams);default:return!Sg(t,n)}}function Ya(t,n,e,i){let r=es(t),o=t.value;Object.entries(r).forEach(([s,a])=>{o.component?n?Ya(a,n.children.getContext(s),n.children,i):Ya(a,null,null,i):Ya(a,e?e.getContext(s):null,e,i)}),o.component?n&&n.outlet&&n.outlet.isActivated?i.canDeactivateChecks.push(new ns(n.outlet.component,o)):i.canDeactivateChecks.push(new ns(null,o)):i.canDeactivateChecks.push(new ns(null,o))}function sl(t){return typeof t=="function"}function FR(t){return typeof t=="boolean"}function LR(t){return t&&sl(t.canLoad)}function VR(t){return t&&sl(t.canActivate)}function BR(t){return t&&sl(t.canActivateChild)}function jR(t){return t&&sl(t.canDeactivate)}function HR(t){return t&&sl(t.canMatch)}function IC(t){return t instanceof Er||t?.name==="EmptyError"}var ru=Symbol("INITIAL_VALUE");function ls(){return Xe(t=>Ys(t.map(n=>n.pipe(Ae(1),nt(ru)))).pipe(oe(n=>{for(let e of n)if(e!==!0){if(e===ru)return ru;if(e===!1||UR(e))return e}return!0}),ge(n=>n!==ru),Ae(1)))}function UR(t){return is(t)||t instanceof as}function MC(t){return t.aborted?Y(void 0).pipe(Ae(1)):new ne(n=>{let e=()=>{n.next(),n.complete()};return t.addEventListener("abort",e),()=>t.removeEventListener("abort",e)})}function TC(t){return ye(MC(t))}function zR(t){return Tt(n=>{let{targetSnapshot:e,currentSnapshot:i,guards:{canActivateChecks:r,canDeactivateChecks:o}}=n;return o.length===0&&r.length===0?Y(k(v({},n),{guardsResult:!0})):$R(o,e,i).pipe(Tt(s=>s&&FR(s)?GR(e,r,t):Y(s)),oe(s=>k(v({},n),{guardsResult:s})))})}function $R(t,n,e){return Ve(t).pipe(Tt(i=>XR(i.component,i.route,e,n)),Kn(i=>i!==!0,!0))}function GR(t,n,e){return Ve(n).pipe(No(i=>Ni(qR(i.route.parent,e),WR(i.route,e),ZR(t,i.path),YR(t,i.route))),Kn(i=>i!==!0,!0))}function WR(t,n){return t!==null&&n&&n(new _u(t)),Y(!0)}function qR(t,n){return t!==null&&n&&n(new vu(t)),Y(!0)}function YR(t,n){let e=n.routeConfig?n.routeConfig.canActivate:null;if(!e||e.length===0)return Y(!0);let i=e.map(r=>cn(()=>{let o=n._environmentInjector,s=cs(r,o),a=VR(s)?s.canActivate(n,t):ot(o,()=>s(n,t));return io(a).pipe(Kn())}));return Y(i).pipe(ls())}function ZR(t,n){let e=n[n.length-1],r=n.slice(0,n.length-1).reverse().map(o=>RR(o)).filter(o=>o!==null).map(o=>cn(()=>{let s=o.guards.map(a=>{let l=o.node._environmentInjector,c=cs(a,l),u=BR(c)?c.canActivateChild(e,t):ot(l,()=>c(e,t));return io(u).pipe(Kn())});return Y(s).pipe(ls())}));return Y(r).pipe(ls())}function XR(t,n,e,i){let r=n&&n.routeConfig?n.routeConfig.canDeactivate:null;if(!r||r.length===0)return Y(!0);let o=r.map(s=>{let a=n._environmentInjector,l=cs(s,a),c=jR(l)?l.canDeactivate(t,n,e,i):ot(a,()=>l(t,n,e,i));return io(c).pipe(Kn())});return Y(o).pipe(ls())}function KR(t,n,e,i,r){let o=n.canLoad;if(o===void 0||o.length===0)return Y(!0);let s=o.map(a=>{let l=cs(a,t),c=LR(l)?l.canLoad(n,e):ot(t,()=>l(n,e)),u=io(c);return r?u.pipe(TC(r)):u});return Y(s).pipe(ls(),NC(i))}function NC(t){return ic(dt(n=>{if(typeof n!="boolean")throw Du(t,n)}),oe(n=>n===!0))}function QR(t,n,e,i,r,o){let s=n.canMatch;if(!s||s.length===0)return Y(!0);let a=s.map(l=>{let c=cs(l,t),u=HR(c)?c.canMatch(n,e,r):ot(t,()=>c(n,e,r));return io(u).pipe(TC(o))});return Y(a).pipe(ls(),NC(i))}var mi=class t extends Error{segmentGroup;constructor(n){super(),this.segmentGroup=n||null,Object.setPrototypeOf(this,t.prototype)}},nl=class t extends Error{urlTree;constructor(n){super(),this.urlTree=n,Object.setPrototypeOf(this,t.prototype)}};function JR(t){throw new x(4e3,!1)}function eO(t){throw wC(!1,xt.GuardRejected)}var Eg=class{urlSerializer;urlTree;constructor(n,e){this.urlSerializer=n,this.urlTree=e}async lineralizeSegments(n,e){let i=[],r=e.root;for(;;){if(i=i.concat(r.segments),r.numberOfChildren===0)return i;if(r.numberOfChildren>1||!r.children[se])throw JR(`${n.redirectTo}`);r=r.children[se]}}async applyRedirectCommands(n,e,i,r,o){let s=await tO(e,r,o);if(s instanceof nn)throw new nl(s);let a=this.applyRedirectCreateUrlTree(s,this.urlSerializer.parse(s),n,i);if(s[0]==="/")throw new nl(a);return a}applyRedirectCreateUrlTree(n,e,i,r){let o=this.createSegmentGroup(n,e.root,i,r);return new nn(o,this.createQueryParams(e.queryParams,this.urlTree.queryParams),e.fragment)}createQueryParams(n,e){let i={};return Object.entries(n).forEach(([r,o])=>{if(typeof o=="string"&&o[0]===":"){let a=o.substring(1);i[r]=e[a]}else i[r]=o}),i}createSegmentGroup(n,e,i,r){let o=this.createSegments(n,e.segments,i,r),s=Object.create(null);return Object.entries(e.children).forEach(([a,l])=>{s[a]=this.createSegmentGroup(n,l,i,r)}),new Se(o,s)}createSegments(n,e,i,r){return e.map(o=>o.path[0]===":"?this.findPosParam(n,o,r):this.findOrReturn(o,i))}findPosParam(n,e,i){let r=i[e.path.substring(1)];if(!r)throw new x(4001,!1);return r}findOrReturn(n,e){let i=0;for(let r of e){if(r.path===n.path)return e.splice(i),r;i++}return n}};function tO(t,n,e){if(typeof t=="string")return Promise.resolve(t);let i=t;return lu(io(ot(e,()=>i(n))))}function nO(t,n){return t.providers&&!t._injector&&(t._injector=ka(t.providers,n,`Route: ${t.path}`)),t._injector??n}function Sn(t){return t.outlet||se}function iO(t,n){let e=t.filter(i=>Sn(i)===n);return e.push(...t.filter(i=>Sn(i)!==n)),e}var wg={matched:!1,consumedSegments:[],remainingSegments:[],parameters:{},positionalParamSegments:{}};function kC(t){return{routeConfig:t.routeConfig,url:t.url,params:t.params,queryParams:t.queryParams,fragment:t.fragment,data:t.data,outlet:t.outlet,title:t.title,paramMap:t.paramMap,queryParamMap:t.queryParamMap}}function rO(t,n,e,i,r,o,s){let a=AC(t,n,e);if(!a.matched)return Y(a);let l=kC(o(a));return i=nO(n,i),QR(i,n,e,r,l,s).pipe(oe(c=>c===!0?a:v({},wg)))}function AC(t,n,e){if(n.path==="")return n.pathMatch==="full"&&(t.hasChildren()||e.length>0)?v({},wg):{matched:!0,consumedSegments:[],remainingSegments:e,parameters:{},positionalParamSegments:{}};let r=(n.matcher||nC)(e,t,n);if(!r)return v({},wg);let o={};Object.entries(r.posParams??{}).forEach(([a,l])=>{o[a]=l.path});let s=r.consumed.length>0?v(v({},o),r.consumed[r.consumed.length-1].parameters):o;return{matched:!0,consumedSegments:r.consumed,remainingSegments:e.slice(r.consumed.length),parameters:s,positionalParamSegments:r.posParams??{}}}function eC(t,n,e,i,r){return e.length>0&&aO(t,e,i,r)?{segmentGroup:new Se(n,sO(i,new Se(e,t.children))),slicedSegments:[]}:e.length===0&&lO(t,e,i)?{segmentGroup:new Se(t.segments,oO(t,e,i,t.children)),slicedSegments:e}:{segmentGroup:new Se(t.segments,t.children),slicedSegments:e}}function oO(t,n,e,i){let r={};for(let o of e)if(Iu(t,n,o)&&!i[Sn(o)]){let s=new Se([],{});r[Sn(o)]=s}return v(v({},i),r)}function sO(t,n){let e={};e[se]=n;for(let i of t)if(i.path===""&&Sn(i)!==se){let r=new Se([],{});e[Sn(i)]=r}return e}function aO(t,n,e,i){return e.some(r=>!Iu(t,n,r)||!(Sn(r)!==se)?!1:!(i!==void 0&&Sn(r)===i))}function lO(t,n,e){return e.some(i=>Iu(t,n,i))}function Iu(t,n,e){return(t.hasChildren()||n.length>0)&&e.pathMatch==="full"?!1:e.path===""}function cO(t,n,e){return n.length===0&&!t.children[e]}var xg=class{};async function dO(t,n,e,i,r,o,s,a){return new Ig(t,n,e,i,r,s,o,a).recognize()}var uO=31,Ig=class{injector;configLoader;rootComponentType;config;urlTree;paramsInheritanceStrategy;urlSerializer;abortSignal;applyRedirects;absoluteRedirectCount=0;allowRedirects=!0;constructor(n,e,i,r,o,s,a,l){this.injector=n,this.configLoader=e,this.rootComponentType=i,this.config=r,this.urlTree=o,this.paramsInheritanceStrategy=s,this.urlSerializer=a,this.abortSignal=l,this.applyRedirects=new Eg(this.urlSerializer,this.urlTree)}noMatchError(n){return new x(4002,`'${n.segmentGroup}'`)}async recognize(){let n=eC(this.urlTree.root,[],[],this.config).segmentGroup,{children:e,rootSnapshot:i}=await this.match(n),r=new Ut(i,e),o=new el("",r),s=mC(i,[],this.urlTree.queryParams,this.urlTree.fragment);return s.queryParams=this.urlTree.queryParams,o.url=this.urlSerializer.serialize(s),{state:o,tree:s}}async match(n){let e=new ss([],Object.freeze({}),Object.freeze(v({},this.urlTree.queryParams)),this.urlTree.fragment,Object.freeze({}),se,this.rootComponentType,null,{},this.injector);try{return{children:await this.processSegmentGroup(this.injector,this.config,n,se,e),rootSnapshot:e}}catch(i){if(i instanceof nl)return this.urlTree=i.urlTree,this.match(i.urlTree.root);throw i instanceof mi?this.noMatchError(i):i}}async processSegmentGroup(n,e,i,r,o){if(i.segments.length===0&&i.hasChildren())return this.processChildren(n,e,i,o);let s=await this.processSegment(n,e,i,i.segments,r,!0,o);return s instanceof Ut?[s]:[]}async processChildren(n,e,i,r){let o=[];for(let l of Object.keys(i.children))l==="primary"?o.unshift(l):o.push(l);let s=[];for(let l of o){let c=i.children[l],u=iO(e,l),f=await this.processSegmentGroup(n,u,c,l,r);s.push(...f)}let a=RC(s);return fO(a),a}async processSegment(n,e,i,r,o,s,a){for(let l of e)try{return await this.processSegmentAgainstRoute(l._injector??n,e,l,i,r,o,s,a)}catch(c){if(c instanceof mi||IC(c))continue;throw c}if(cO(i,r,o))return new xg;throw new mi(i)}async processSegmentAgainstRoute(n,e,i,r,o,s,a,l){if(Sn(i)!==s&&(s===se||!Iu(r,o,i)))throw new mi(r);if(i.redirectTo===void 0)return this.matchSegmentAgainstRoute(n,r,i,o,s,l);if(this.allowRedirects&&a)return this.expandSegmentAgainstRouteUsingRedirect(n,r,e,i,o,s,l);throw new mi(r)}async expandSegmentAgainstRouteUsingRedirect(n,e,i,r,o,s,a){let{matched:l,parameters:c,consumedSegments:u,positionalParamSegments:f,remainingSegments:h}=AC(e,r,o);if(!l)throw new mi(e);typeof r.redirectTo=="string"&&r.redirectTo[0]==="/"&&(this.absoluteRedirectCount++,this.absoluteRedirectCount>uO&&(this.allowRedirects=!1));let p=this.createSnapshot(n,r,o,c,a);if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let S=await this.applyRedirects.applyRedirectCommands(u,r.redirectTo,f,kC(p),n),C=await this.applyRedirects.lineralizeSegments(r,S);return this.processSegment(n,i,e,C.concat(h),s,!1,a)}createSnapshot(n,e,i,r,o){let s=new ss(i,r,Object.freeze(v({},this.urlTree.queryParams)),this.urlTree.fragment,mO(e),Sn(e),e.component??e._loadedComponent??null,e,pO(e),n),a=Tg(s,o,this.paramsInheritanceStrategy);return s.params=Object.freeze(a.params),s.data=Object.freeze(a.data),s}async matchSegmentAgainstRoute(n,e,i,r,o,s){if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let a=Je=>this.createSnapshot(n,i,Je.consumedSegments,Je.parameters,s),l=await lu(rO(e,i,r,n,this.urlSerializer,a,this.abortSignal));if(i.path==="**"&&(e.children={}),!l?.matched)throw new mi(e);n=i._injector??n;let{routes:c}=await this.getChildConfig(n,i,r),u=i._loadedInjector??n,{parameters:f,consumedSegments:h,remainingSegments:p}=l,S=this.createSnapshot(n,i,h,f,s),{segmentGroup:C,slicedSegments:$}=eC(e,h,p,c,o);if($.length===0&&C.hasChildren()){let Je=await this.processChildren(u,c,C,S);return new Ut(S,Je)}if(c.length===0&&$.length===0)return new Ut(S,[]);let F=Sn(i)===o,we=await this.processSegment(u,c,C,$,F?se:o,!0,S);return new Ut(S,we instanceof Ut?[we]:[])}async getChildConfig(n,e,i){if(e.children)return{routes:e.children,injector:n};if(e.loadChildren){if(e._loadedRoutes!==void 0){let o=e._loadedNgModuleFactory;return o&&!e._loadedInjector&&(e._loadedInjector=o.create(n).injector),{routes:e._loadedRoutes,injector:e._loadedInjector}}if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);if(await lu(KR(n,e,i,this.urlSerializer,this.abortSignal))){let o=await this.configLoader.loadChildren(n,e);return e._loadedRoutes=o.routes,e._loadedInjector=o.injector,e._loadedNgModuleFactory=o.factory,o}throw eO(e)}return{routes:[],injector:n}}};function fO(t){t.sort((n,e)=>n.value.outlet===se?-1:e.value.outlet===se?1:n.value.outlet.localeCompare(e.value.outlet))}function hO(t){let n=t.value.routeConfig;return n&&n.path===""}function RC(t){let n=[],e=new Set;for(let i of t){if(!hO(i)){n.push(i);continue}let r=n.find(o=>i.value.routeConfig===o.value.routeConfig);r!==void 0?(r.children.push(...i.children),e.add(r)):n.push(i)}for(let i of e){let r=RC(i.children);n.push(new Ut(i.value,r))}return n.filter(i=>!e.has(i))}function mO(t){return t.data||{}}function pO(t){return t.resolve||{}}function gO(t,n,e,i,r,o,s){return Tt(async a=>{let{state:l,tree:c}=await dO(t,n,e,i,a.extractedUrl,r,o,s);return k(v({},a),{targetSnapshot:l,urlAfterRedirects:c})})}function vO(t){return Tt(n=>{let{targetSnapshot:e,guards:{canActivateChecks:i}}=n;if(!i.length)return Y(n);let r=new Set(i.map(a=>a.route)),o=new Set;for(let a of r)if(!o.has(a))for(let l of OC(a))o.add(l);let s=0;return Ve(o).pipe(No(a=>r.has(a)?bO(a,e,t):(a.data=Tg(a,a.parent,t).resolve,Y(void 0))),dt(()=>s++),Mc(1),Tt(a=>s===o.size?Y(n):Ge))})}function OC(t){let n=t.children.map(e=>OC(e)).flat();return[t,...n]}function bO(t,n,e){let i=t.routeConfig,r=t._resolve;return i?.title!==void 0&&!CC(i)&&(r[il]=i.title),cn(()=>(t.data=Tg(t,t.parent,e).resolve,_O(r,t,n).pipe(oe(o=>(t._resolvedData=o,t.data=v(v({},t.data),o),null)))))}function _O(t,n,e){let i=mg(t);if(i.length===0)return Y({});let r={};return Ve(i).pipe(Tt(o=>yO(t[o],n,e).pipe(Kn(),dt(s=>{if(s instanceof as)throw Du(new Zi,s);r[o]=s}))),Mc(1),oe(()=>r),wr(o=>IC(o)?Ge:Ws(o)))}function yO(t,n,e){let i=n._environmentInjector,r=cs(t,i),o=r.resolve?r.resolve(n,e):ot(i,()=>r(n,e));return io(o)}var PC=new b("");function Mg(t){return Xe(n=>{let e=t(n);return e?Ve(e).pipe(oe(()=>n)):Y(n)})}var Rg=(()=>{class t{buildTitle(e){let i,r=e.root;for(;r!==void 0;)i=this.getResolvedTitleForRoute(r)??i,r=r.children.find(o=>o.outlet===se);return i}getResolvedTitleForRoute(e){return e.data[il]}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:()=>d(FC)})}return t})(),FC=(()=>{class t extends Rg{title;constructor(e){super(),this.title=e}updateTitle(e){let i=this.buildTitle(e);i!==void 0&&this.title.setTitle(i)}static \u0275fac=function(i){return new(i||t)(A(q0))};static \u0275prov=G({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),al=new b("",{factory:()=>({})}),ll=new b(""),LC=(()=>{class t{componentLoaders=new WeakMap;childrenLoaders=new WeakMap;onLoadStartListener;onLoadEndListener;compiler=d(Bp);async loadComponent(e,i){if(this.componentLoaders.get(i))return this.componentLoaders.get(i);if(i._loadedComponent)return Promise.resolve(i._loadedComponent);this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let o=await rC(ot(e,()=>i.loadComponent())),s=await BC(zp(o));return this.onLoadEndListener&&this.onLoadEndListener(i),i._loadedComponent=s,s}finally{this.componentLoaders.delete(i)}})();return this.componentLoaders.set(i,r),r}loadChildren(e,i){if(this.childrenLoaders.get(i))return this.childrenLoaders.get(i);if(i._loadedRoutes)return Promise.resolve({routes:i._loadedRoutes,injector:i._loadedInjector});this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let o=await VC(i,this.compiler,e,this.onLoadEndListener);return i._loadedRoutes=o.routes,i._loadedInjector=o.injector,i._loadedNgModuleFactory=o.factory,o}finally{this.childrenLoaders.delete(i)}})();return this.childrenLoaders.set(i,r),r}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();async function VC(t,n,e,i){let r=await rC(ot(e,()=>t.loadChildren())),o=await BC(zp(r)),s;o instanceof Ud||Array.isArray(o)?s=o:s=await n.compileModuleAsync(o),i&&i(t);let a,l,c=!1,u;return Array.isArray(s)?(l=s,c=!0):(a=s.create(e).injector,u=s,l=a.get(ll,[],{optional:!0,self:!0}).flat()),{routes:l.map(Ag),injector:a,factory:u}}async function BC(t){return t}var Mu=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:()=>d(SO)})}return t})(),SO=(()=>{class t{shouldProcessUrl(e){return!0}extract(e){return e}merge(e,i){return e}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})(),jC=new b("");var CO=()=>{},HC=new b(""),UC=(()=>{class t{currentNavigation=j(null,{equal:()=>!1});currentTransition=null;lastSuccessfulNavigation=j(null);events=new E;transitionAbortWithErrorSubject=new E;configLoader=d(LC);environmentInjector=d(Ie);destroyRef=d(We);urlSerializer=d(rl);rootContexts=d(no);location=d(qi);inputBindingEnabled=d(xu,{optional:!0})!==null;titleStrategy=d(Rg);options=d(al,{optional:!0})||{};paramsInheritanceStrategy=this.options.paramsInheritanceStrategy||IR;urlHandlingStrategy=d(Mu);createViewTransition=d(jC,{optional:!0});navigationErrorHandler=d(HC,{optional:!0});routerResourcesFeature=d(PC,{optional:!0});navigationId=0;get hasRequestedNavigation(){return this.navigationId!==0}transitions;afterPreactivation=()=>Y(void 0);rootComponentType=null;destroyed=!1;constructor(){let e=r=>this.events.next(new pu(r)),i=r=>this.events.next(new gu(r));this.configLoader.onLoadEndListener=i,this.configLoader.onLoadStartListener=e,this.destroyRef.onDestroy(()=>{this.destroyed=!0})}complete(){this.transitions?.complete()}handleNavigationRequest(e){let i=++this.navigationId;Ne(()=>{this.transitions?.next(k(v({},e),{extractedUrl:this.urlHandlingStrategy.extract(e.rawUrl),targetSnapshot:null,targetRouterState:null,guards:{canActivateChecks:[],canDeactivateChecks:[]},guardsResult:null,id:i,routesRecognizeHandler:{},beforeActivateHandler:{}}))})}setupNavigations(e){return this.transitions=new tt(null),this.transitions.pipe(ge(i=>i!==null),Xe(i=>{let r=!0,o=!1,s=new AbortController,a=()=>!o&&this.currentTransition?.id===i.id;return Y(i).pipe(Xe(l=>{if(this.navigationId>i.id)return this.cancelNavigationTransition(i,"",xt.SupersededByNewNavigation),Ge;this.currentTransition=i;let c=this.lastSuccessfulNavigation();this.currentNavigation.set({id:l.id,initialUrl:l.rawUrl,extractedUrl:l.extractedUrl,targetBrowserUrl:typeof l.extras.browserUrl=="string"?this.urlSerializer.parse(l.extras.browserUrl):l.extras.browserUrl,trigger:l.source,extras:l.extras,previousNavigation:c?k(v({},c),{previousNavigation:null}):null,abort:()=>s.abort(),routesRecognizeHandler:l.routesRecognizeHandler,beforeActivateHandler:l.beforeActivateHandler});let u=!e.navigated||this.isUpdatingInternalState()||this.isUpdatedBrowserUrl(),f=l.extras.onSameUrlNavigation??e.onSameUrlNavigation;if(!u&&f!=="reload")return this.events.next(new gi(l.id,this.urlSerializer.serialize(l.rawUrl),"",Xa.IgnoredSameUrlNavigation)),l.resolve(!1),Ge;if(this.urlHandlingStrategy.shouldProcessUrl(l.rawUrl))return Y(l).pipe(Xe(h=>(this.events.next(new eo(h.id,this.urlSerializer.serialize(h.extractedUrl),h.source,h.restoredState)),h.id!==this.navigationId?Ge:Promise.resolve(h))),gO(this.environmentInjector,this.configLoader,this.rootComponentType,e.config,this.urlSerializer,this.paramsInheritanceStrategy,s.signal),dt(h=>{i.targetSnapshot=h.targetSnapshot,i.urlAfterRedirects=h.urlAfterRedirects,this.currentNavigation.update(p=>(p.finalUrl=h.urlAfterRedirects,p)),this.events.next(new Qa)}),Xe(h=>Ve(i.routesRecognizeHandler.deferredHandle??Y(void 0)).pipe(oe(()=>h))),dt(()=>{let h=new Ka(l.id,this.urlSerializer.serialize(l.extractedUrl),this.urlSerializer.serialize(l.urlAfterRedirects),l.targetSnapshot);this.events.next(h)}));if(u&&this.urlHandlingStrategy.shouldProcessUrl(l.currentRawUrl)){let{id:h,extractedUrl:p,source:S,restoredState:C,extras:$}=l,F=new eo(h,this.urlSerializer.serialize(p),S,C);this.events.next(F);let we=yC(this.rootComponentType,this.environmentInjector).snapshot;return this.currentTransition=i=k(v({},l),{targetSnapshot:we,urlAfterRedirects:p,extras:k(v({},$),{skipLocationChange:!1,replaceUrl:!1})}),this.currentNavigation.update(Je=>(Je.finalUrl=p,Je)),Y(i)}else return this.events.next(new gi(l.id,this.urlSerializer.serialize(l.extractedUrl),"",Xa.IgnoredByUrlHandlingStrategy)),l.resolve(!1),Ge}),oe(l=>{let c=new uu(l.id,this.urlSerializer.serialize(l.extractedUrl),this.urlSerializer.serialize(l.urlAfterRedirects),l.targetSnapshot);return this.events.next(c),this.currentTransition=i=k(v({},l),{guards:AR(l.targetSnapshot,l.currentSnapshot,this.rootContexts)}),i}),zR(l=>this.events.next(l)),Xe(l=>{if(i.guardsResult=l.guardsResult,l.guardsResult&&typeof l.guardsResult!="boolean")throw Du(this.urlSerializer,l.guardsResult);let c=new fu(l.id,this.urlSerializer.serialize(l.extractedUrl),this.urlSerializer.serialize(l.urlAfterRedirects),l.targetSnapshot,!!l.guardsResult);if(this.events.next(c),!a())return Ge;if(!l.guardsResult)return this.cancelNavigationTransition(l,"",xt.GuardRejected),Ge;if(l.guards.canActivateChecks.length===0)return Y(l);let u=new hu(l.id,this.urlSerializer.serialize(l.extractedUrl),this.urlSerializer.serialize(l.urlAfterRedirects),l.targetSnapshot);if(this.events.next(u),!a())return Ge;let f=!1;return Y(l).pipe(vO(this.paramsInheritanceStrategy),dt({next:()=>{f=!0;let h=new mu(l.id,this.urlSerializer.serialize(l.extractedUrl),this.urlSerializer.serialize(l.urlAfterRedirects),l.targetSnapshot);this.events.next(h)},complete:()=>{f||this.cancelNavigationTransition(l,"",xt.NoDataFromResolver)}}))}),Mg(l=>{let c=f=>{let h=[];if(f.routeConfig?._loadedComponent)f.component=f.routeConfig?._loadedComponent;else if(f.routeConfig?.loadComponent){let p=f._environmentInjector;h.push(this.configLoader.loadComponent(p,f.routeConfig).then(S=>{f.component=S}))}for(let p of f.children)h.push(...c(p));return h},u=c(l.targetSnapshot.root);return u.length===0?Y(l):Ve(Promise.all(u).then(()=>l))}),Xe(l=>{let{newlyCreatedRoutes:c,state:u}=MR(e.routeReuseStrategy,l.targetSnapshot,l.currentRouterState);return this.currentTransition=i=l=k(v({},l),{targetRouterState:u,newlyCreatedRoutes:c}),this.currentNavigation.update(f=>(f.targetRouterState=u,f)),Y(l)}),this.routerResourcesFeature?.setupAndRunResources(s.signal)??(l=>l),Mg(()=>this.afterPreactivation()),Xe(()=>{let{currentSnapshot:l,targetSnapshot:c}=i,u=this.createViewTransition?.(this.environmentInjector,l.root,c.root,i.hasUAVisualTransition);return u?Ve(u).pipe(oe(()=>i)):Y(i)}),Ae(1),Xe(l=>{r=!1,this.events.next(new rs);let c=i.beforeActivateHandler.deferredHandle;return c?Ve(c.then(()=>l)):Y(l)}),dt(l=>{new Dg(e.routeReuseStrategy,i.targetRouterState,i.currentRouterState,c=>this.events.next(c),this.inputBindingEnabled).activate(this.rootContexts),l.newlyCreatedRoutes?.clear(),a()&&(zC(l.targetRouterState),o=!0,this.currentNavigation.update(c=>(c.abort=CO,c)),this.lastSuccessfulNavigation.set(Ne(this.currentNavigation)),this.events.next(new pi(l.id,this.urlSerializer.serialize(l.extractedUrl),this.urlSerializer.serialize(l.urlAfterRedirects))),this.titleStrategy?.updateTitle(l.targetRouterState.snapshot),l.resolve(!0))}),ye(MC(s.signal).pipe(ge(()=>!o&&r),dt(()=>{this.cancelNavigationTransition(i,s.signal.reason+"",xt.Aborted)}))),dt({complete:()=>{o=!0}}),ye(this.transitionAbortWithErrorSubject.pipe(dt(l=>{throw l}))),xr(()=>{s.abort(),o||this.cancelNavigationTransition(i,"",xt.SupersededByNewNavigation),this.currentTransition?.id===i.id&&(this.currentNavigation.set(null),this.currentTransition=null)}),wr(l=>{if(o=!0,tC(i),this.destroyed)return i.resolve(!1),Ge;if(xC(l))this.events.next(new tn(i.id,this.urlSerializer.serialize(i.extractedUrl),l.message,l.cancellationCode)),kR(l)?this.events.next(new os(l.url,l.navigationBehaviorOptions)):i.resolve(!1);else{let c=new to(i.id,this.urlSerializer.serialize(i.extractedUrl),l,i.targetSnapshot??void 0);try{let u=ot(this.environmentInjector,()=>this.navigationErrorHandler?.(c));if(u instanceof as){let{message:f,cancellationCode:h}=Du(this.urlSerializer,u);this.events.next(new tn(i.id,this.urlSerializer.serialize(i.extractedUrl),f,h)),this.events.next(new os(u.redirectTo,u.navigationBehaviorOptions))}else throw this.events.next(c),l}catch(u){this.options.resolveNavigationPromiseOnError?i.resolve(!1):i.reject(u)}}return Ge}))}))}cancelNavigationTransition(e,i,r){tC(e);let o=new tn(e.id,this.urlSerializer.serialize(e.extractedUrl),i,r);this.events.next(o),e.resolve(!1)}isUpdatingInternalState(){return this.currentTransition?.extractedUrl.toString()!==this.currentTransition?.currentUrlTree.toString()}isUpdatedBrowserUrl(){let e=this.urlHandlingStrategy.extract(this.urlSerializer.parse(this.location.path(!0))),i=Ne(this.currentNavigation),r=i?.targetBrowserUrl??i?.extractedUrl;return e.toString()!==r?.toString()&&!i?.extras.skipLocationChange}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();function DO(t){return t!==qa}function tC(t){for(let n of t.newlyCreatedRoutes??[])n._localInjector?.destroy(),n._localInjector=void 0;zC(t.targetRouterState)}function zC(t){if(!t)return;let n=e=>{e.value.pending?.set(!1),e.children.forEach(n)};n(t._root)}var $C=new b("");var GC=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:()=>d(EO)})}return t})(),wu=class{shouldDetach(n){return!1}store(n,e){}shouldAttach(n){return!1}retrieve(n){return null}shouldReuseRoute(n,e){return n.routeConfig===e.routeConfig}shouldDestroyInjector(n){return!0}},EO=(()=>{class t extends wu{static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})(),Og=(()=>{class t{urlSerializer=d(rl);options=d(al,{optional:!0})||{};canceledNavigationResolution=this.options.canceledNavigationResolution||"replace";location=d(qi);urlHandlingStrategy=d(Mu);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";currentUrlTree=new nn;getCurrentUrlTree(){return this.currentUrlTree}rawUrlTree=this.currentUrlTree;getRawUrlTree(){return this.rawUrlTree}createBrowserPath({finalUrl:e,initialUrl:i,targetBrowserUrl:r}){let o=e!==void 0?this.urlHandlingStrategy.merge(e,i):i,s=r??o;return s instanceof nn?this.urlSerializer.serialize(s):s}routerUrlState(e){return e?.targetBrowserUrl===void 0||e?.finalUrl===void 0?{}:{\u0275routerUrl:this.urlSerializer.serialize(e.finalUrl)}}commitTransition({targetRouterState:e,finalUrl:i,initialUrl:r}){i&&e?(this.currentUrlTree=i,this.rawUrlTree=this.urlHandlingStrategy.merge(i,r),this.routerState=e):this.rawUrlTree=r}routerState=yC(null,d(Ie));getRouterState(){return this.routerState}_stateMemento=this.createStateMemento();get stateMemento(){return this._stateMemento}updateStateMemento(){this._stateMemento=this.createStateMemento()}createStateMemento(){return{rawUrlTree:this.rawUrlTree,currentUrlTree:this.currentUrlTree,routerState:this.routerState}}restoredState(){return this.location.getState()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:()=>d(wO)})}return t})(),wO=(()=>{class t extends Og{currentPageId=0;lastSuccessfulId=-1;get browserPageId(){return this.canceledNavigationResolution!=="computed"?this.currentPageId:this.restoredState()?.\u0275routerPageId??this.currentPageId}registerNonRouterCurrentEntryChangeListener(e){return this.location.subscribe(i=>{i.type==="popstate"&&setTimeout(()=>{e(i.url,i.state,"popstate",{replaceUrl:!0},i.hasUAVisualTransition)})})}handleRouterEvent(e,i){e instanceof eo?this.updateStateMemento():e instanceof gi?this.commitTransition(i):e instanceof Ka?this.urlUpdateStrategy==="eager"&&(i.extras.skipLocationChange||this.setBrowserUrl(this.createBrowserPath(i),i)):e instanceof rs?(this.commitTransition(i),this.urlUpdateStrategy==="deferred"&&!i.extras.skipLocationChange&&this.setBrowserUrl(this.createBrowserPath(i),i)):e instanceof tn&&!_C(e)?this.restoreHistory(i):e instanceof to?this.restoreHistory(i,!0):e instanceof pi&&(this.lastSuccessfulId=e.id,this.currentPageId=this.browserPageId)}setBrowserUrl(e,i){let{extras:r,id:o}=i,{replaceUrl:s,state:a}=r;if(this.location.isCurrentPathEqualTo(e)||s){let l=this.browserPageId,c=v(v({},a),this.generateNgRouterState(o,l,i));this.location.replaceState(e,"",c)}else{let l=v(v({},a),this.generateNgRouterState(o,this.browserPageId+1,i));this.location.go(e,"",l)}}restoreHistory(e,i=!1){if(this.canceledNavigationResolution==="computed"){let r=this.browserPageId,o=this.currentPageId-r;o!==0?this.location.historyGo(o):this.getCurrentUrlTree()===e.finalUrl&&o===0&&(this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}else this.canceledNavigationResolution==="replace"&&(i&&this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}resetInternalState({finalUrl:e}){this.routerState=this.stateMemento.routerState,this.currentUrlTree=this.stateMemento.currentUrlTree,this.rawUrlTree=this.urlHandlingStrategy.merge(this.currentUrlTree,e??this.rawUrlTree)}resetUrlToCurrentUrlTree(){this.location.replaceState(this.urlSerializer.serialize(this.getRawUrlTree()),"",this.generateNgRouterState(this.lastSuccessfulId,this.currentPageId))}generateNgRouterState(e,i,r){return this.canceledNavigationResolution==="computed"?v({navigationId:e,\u0275routerPageId:i},this.routerUrlState(r)):v({navigationId:e},this.routerUrlState(r))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();function Pg(t,n){t.events.pipe(ge(e=>e instanceof pi||e instanceof tn||e instanceof to||e instanceof gi),oe(e=>e instanceof pi||e instanceof gi?0:(e instanceof tn?e.code===xt.Redirect||e.code===xt.SupersededByNewNavigation:!1)?2:1),ge(e=>e!==2),Ae(1)).subscribe(()=>{n()})}var Tu=(()=>{class t{get currentUrlTree(){return this.stateManager.getCurrentUrlTree()}get rawUrlTree(){return this.stateManager.getRawUrlTree()}disposed=!1;nonRouterCurrentEntryChangeSubscription;console=d($d);stateManager=d(Og);options=d(al,{optional:!0})||{};pendingTasks=d(oi);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";navigationTransitions=d(UC);urlSerializer=d(rl);location=d(qi);urlHandlingStrategy=d(Mu);injector=d(Ie);_events=new E;get events(){return this._events}get routerState(){return this.stateManager.getRouterState()}navigated=!1;routeReuseStrategy=d(GC);injectorCleanup=d($C,{optional:!0});onSameUrlNavigation=this.options.onSameUrlNavigation||"ignore";config=d(ll,{optional:!0})?.flat()??[];componentInputBindingEnabled=!!d(xu,{optional:!0});currentNavigation=this.navigationTransitions.currentNavigation.asReadonly();constructor(){this.resetConfig(this.config),this.navigationTransitions.setupNavigations(this).subscribe({error:e=>{}}),this.subscribeToNavigationEvents()}eventsSubscription=new pe;subscribeToNavigationEvents(){let e=this.navigationTransitions.events.subscribe(i=>{try{let r=this.navigationTransitions.currentTransition,o=Ne(this.navigationTransitions.currentNavigation);if(r!==null&&o!==null){if(this.stateManager.handleRouterEvent(i,o),i instanceof tn&&i.code!==xt.Redirect&&i.code!==xt.SupersededByNewNavigation)this.navigated=!0;else if(i instanceof pi)this.navigated=!0,this.injectorCleanup?.(this.routeReuseStrategy,this.routerState,this.config);else if(i instanceof os){let s=i.navigationBehaviorOptions,a=this.urlHandlingStrategy.merge(i.url,r.currentRawUrl),l=v({scroll:r.extras.scroll,browserUrl:r.extras.browserUrl,info:r.extras.info,skipLocationChange:r.extras.skipLocationChange,replaceUrl:r.extras.replaceUrl||this.urlUpdateStrategy==="eager"||DO(r.source)},s);this.scheduleNavigation(a,qa,null,l,r.hasUAVisualTransition,{resolve:r.resolve,reject:r.reject,promise:r.promise})}}wR(i)&&this._events.next(i)}catch(r){this.navigationTransitions.transitionAbortWithErrorSubject.next(r)}});this.eventsSubscription.add(e)}resetRootComponentType(e){this.routerState.root.component=e,this.navigationTransitions.rootComponentType=e}initialNavigation(){this.setUpLocationChangeListener(),this.navigationTransitions.hasRequestedNavigation||this.navigateToSyncWithBrowser(this.location.path(!0),qa,this.stateManager.restoredState(),{replaceUrl:!0})}setUpLocationChangeListener(){this.nonRouterCurrentEntryChangeSubscription??=this.stateManager.registerNonRouterCurrentEntryChangeListener((e,i,r,o,s)=>{this.navigateToSyncWithBrowser(e,r,i,o,s)})}navigateToSyncWithBrowser(e,i,r,o,s){let a=r?.navigationId?r:null,l=r?.\u0275routerUrl??e;if(r?.\u0275routerUrl&&(o=k(v({},o),{browserUrl:e})),r){let u=v({},r);delete u.navigationId,delete u.\u0275routerPageId,delete u.\u0275routerUrl,Object.keys(u).length!==0&&(o.state=u)}let c=this.parseUrl(l);this.scheduleNavigation(c,i,a,o,s).catch(u=>{this.disposed||this.injector.get(Yt)(u)})}get url(){return this.serializeUrl(this.currentUrlTree)}getCurrentNavigation(){return Ne(this.navigationTransitions.currentNavigation)}get lastSuccessfulNavigation(){return this.navigationTransitions.lastSuccessfulNavigation}resetConfig(e){this.config=e.map(Ag),this.navigated=!1}ngOnDestroy(){this.dispose()}dispose(){this._events.unsubscribe(),this.navigationTransitions.complete(),this.nonRouterCurrentEntryChangeSubscription?.unsubscribe(),this.nonRouterCurrentEntryChangeSubscription=void 0,this.disposed=!0,this.eventsSubscription.unsubscribe()}createUrlTree(e,i={}){let{relativeTo:r,queryParams:o,fragment:s,queryParamsHandling:a,preserveFragment:l}=i,c=l?this.currentUrlTree.fragment:s,u=null;switch(a??this.options.defaultQueryParamsHandling){case"merge":u=v(v({},this.currentUrlTree.queryParams),o);break;case"preserve":u=this.currentUrlTree.queryParams;break;default:u=o||null}u!==null&&(u=this.removeEmptyProps(u));let f;try{let h=r?r.snapshot:this.routerState.snapshot.root;f=pC(h)}catch{(typeof e[0]!="string"||e[0][0]!=="/")&&(e=[]),f=this.currentUrlTree.root}return gC(f,e,u,c??null,this.urlSerializer)}navigateByUrl(e,i={skipLocationChange:!1}){let r=is(e)?e:this.parseUrl(e),o=this.urlHandlingStrategy.merge(r,this.rawUrlTree);return this.scheduleNavigation(o,qa,null,i)}navigate(e,i={skipLocationChange:!1}){return xO(e),this.navigateByUrl(this.createUrlTree(e,i),i)}serializeUrl(e){return this.urlSerializer.serialize(e)}parseUrl(e){try{return this.urlSerializer.parse(e)}catch{return this.console.warn(ti(4018,!1)),this.urlSerializer.parse("/")}}isActive(e,i){let r;if(i===!0?r=v({},sC):i===!1?r=v({},pg):r=v(v({},pg),i),is(e))return Z0(this.currentUrlTree,e,r);let o=this.parseUrl(e);return Z0(this.currentUrlTree,o,r)}removeEmptyProps(e){return Object.entries(e).reduce((i,[r,o])=>(o!=null&&(i[r]=o),i),{})}scheduleNavigation(e,i,r,o,s,a){if(this.disposed)return Promise.resolve(!1);let l,c,u;a?(l=a.resolve,c=a.reject,u=a.promise):u=new Promise((h,p)=>{l=h,c=p});let f=this.pendingTasks.add();return Pg(this,()=>{queueMicrotask(()=>this.pendingTasks.remove(f))}),this.navigationTransitions.handleNavigationRequest({source:i,restoredState:r,currentUrlTree:this.currentUrlTree,currentRawUrl:this.currentUrlTree,rawUrl:e,extras:o,hasUAVisualTransition:s,resolve:l,reject:c,promise:u,currentSnapshot:this.routerState.snapshot,currentRouterState:this.routerState}),u.catch(Promise.reject.bind(Promise))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();function xO(t){for(let n=0;n<t.length;n++)if(t[n]==null)throw new x(4008,!1)}var TO=new b("");function Fg(t,...n){return On([{provide:ll,multi:!0,useValue:t},{provide:Xi,useFactory:NO},{provide:Aa,multi:!0,useFactory:kO},n.map(e=>e.\u0275providers)])}function NO(){return d(Tu).routerState.root}function kO(){let t=d(R);return n=>{let e=t.get(Ft);if(n!==e.components[0])return;let i=t.get(Tu),r=t.get(AO);t.get(RO)===1&&i.initialNavigation(),t.get(OO,null,{optional:!0})?.setUpPreloading(),t.get(TO,null,{optional:!0})?.init(),i.resetRootComponentType(e.componentTypes[0]),r.closed||(r.next(),r.complete(),r.unsubscribe())}}var AO=new b("",{factory:()=>new E}),RO=new b("",{factory:()=>1});var OO=new b("");var Nu=new b("LABEL_RENDERER");var Lg=7.9921259842519685,cl=[{key:"50x30",label:"50 x 30 mm",widthMm:50,heightMm:30},{key:"60x40",label:"60 x 40 mm",widthMm:60,heightMm:40},{key:"80x50",label:"80 x 50 mm",widthMm:80,heightMm:50},{key:"100x50",label:"100 x 50 mm",widthMm:100,heightMm:50},{key:"102x152",label:'102 x 152 mm (4x6")',widthMm:102,heightMm:152}],WC=cl[1].key;function dl(t){return cl.find(e=>e.key===t)??cl[1]}function Ki(t){return Math.round(t.widthMm*Lg)}function Qi(t){return Math.round(t.heightMm*Lg)}function qC(t){return{sizePreset:t,elements:[]}}function ku(t,n){return k(v({},t),{elements:[...t.elements,n]})}function YC(t,n){return k(v({},t),{elements:t.elements.filter(e=>e.id!==n)})}function ZC(t,n,e){return k(v({},t),{elements:t.elements.map(i=>i.id===n?v(v({},i),e):i)})}function XC(t,n,e){let i=Math.ceil(t/8),r=new Uint8Array(i*n);for(let o=0;o<n;o++){let s=o*i;for(let a=0;a<t;a++){if(!e(a,o))continue;let l=s+(a>>3),c=7-a%8;r[l]|=1<<c}}return{widthDots:t,heightDots:n,bytesPerRow:i,packedRows:r}}function Vg(){return`${Date.now().toString(36)}-${Math.random().toString(36).slice(2,10)}`}function KC(t){let n=t.widthDots??200,e=t.heightDots??80;return{id:Vg(),kind:"text",xDots:Math.round(t.centerXDots-n/2),yDots:Math.round(t.centerYDots-e/2),widthDots:n,heightDots:e,text:"Novo texto",fontSizeDots:28,bold:!1,align:"left",verticalAlign:"top",rotationDeg:0}}function QC(t,n){return k(v({},t),{id:Vg(),xDots:n.xDots,yDots:n.yDots})}function JC(t){let n=t.naturalWidth/t.naturalHeight,e=Math.min(t.naturalWidth,t.maxWidthDots),i=e/n;return i>t.maxHeightDots&&(i=t.maxHeightDots,e=i*n),e=Math.max(8,Math.round(e)),i=Math.max(8,Math.round(i)),{id:Vg(),kind:"image",xDots:Math.round(t.centerXDots-e/2),yDots:Math.round(t.centerYDots-i/2),widthDots:e,heightDots:i,imageDataUrl:t.imageDataUrl}}function Ru(t,n,e){let i=[];for(let r of t.split(`
`)){let o=r.split(" "),s="";for(let a of o){let l=s?`${s} ${a}`:a;if(e(l)<=n){s=l;continue}if(s&&(i.push(s),s=""),e(a)>n){let c="";for(let u of a){let f=c+u;c&&e(f)>n?(i.push(c),c=u):c=f}s=c}else s=a}i.push(s)}return i}function Bg(t){return Math.round(t*1.2)}function Ou(t,n){return Math.max(1,Math.floor(t/Bg(n)))}function Pu(t,n,e){return e===90||e===270?{width:n,height:t}:{width:t,height:n}}var eD=128,Fu=class t{async render(n){let e=Ki(n.sizePreset),i=Qi(n.sizePreset),r=document.createElement("canvas");r.width=e,r.height=i;let o=r.getContext("2d",{willReadFrequently:!0});if(!o)throw new Error("N\xE3o foi poss\xEDvel criar o contexto de desenho.");o.fillStyle="#ffffff",o.fillRect(0,0,e,i),o.fillStyle="#000000";for(let a of n.elements)a.kind==="text"?this.drawText(o,a):await this.drawImage(o,a);let s=o.getImageData(0,0,e,i);return XC(e,i,(a,l)=>{let c=(l*e+a)*4;if(s.data[c+3]<eD)return!1;let f=s.data[c],h=s.data[c+1],p=s.data[c+2];return .299*f+.587*h+.114*p<eD})}drawText(n,e){let i=e.bold?"bold ":"";n.font=`${i}${e.fontSizeDots}px Arial, sans-serif`,n.textAlign=e.align,n.textBaseline="top";let{width:r,height:o}=Pu(e.widthDots,e.heightDots,e.rotationDeg),s=Bg(e.fontSizeDots),a=Ou(o,e.fontSizeDots),l=Ru(e.text,r,F=>n.measureText(F).width).slice(0,a),c=l.length*s,u=Math.max(0,o-c),f=e.verticalAlign==="middle"?u/2:e.verticalAlign==="bottom"?u:0,h=-r/2,p=-o/2,S=e.align==="center"?0:e.align==="right"?r/2:h,C=e.xDots+e.widthDots/2,$=e.yDots+e.heightDots/2;n.save(),n.translate(C,$),e.rotationDeg!==0&&n.rotate(e.rotationDeg*Math.PI/180),l.forEach((F,we)=>{n.fillText(F,S,p+f+we*s)}),n.restore()}async drawImage(n,e){let i=new Image;i.src=e.imageDataUrl,await i.decode(),n.drawImage(i,e.xDots,e.yDots,e.widthDots,e.heightDots)}static \u0275fac=function(e){return new(e||t)};static \u0275prov=G({token:t,factory:t.\u0275fac,providedIn:"root"})};var ds=new b("PRINTER_GATEWAY");var Ji={host:"127.0.0.1",port:9100};function tD(t){return t.host===Ji.host&&(t.port??9100)===Ji.port}function us(t){try{let n=localStorage.getItem(t);return n?JSON.parse(n):null}catch{return null}}function fs(t,n){try{return localStorage.setItem(t,JSON.stringify(n)),!0}catch{return!1}}function nD(t){try{localStorage.removeItem(t)}catch{}}var jg="zpl-label-studio.printer-connection.v1",er=class t{configSignal=j(us(jg)??Ji);config=this.configSignal.asReadonly();isCustom=Q(()=>!tD(this.configSignal()));update(n){this.configSignal.set(n),fs(jg,n)}reset(){this.configSignal.set(Ji),nD(jg)}static \u0275fac=function(e){return new(e||t)};static \u0275prov=G({token:t,factory:t.\u0275fac,providedIn:"root"})};var ul=class extends Error{},Hg=class extends Error{},Lu=class t{settings=d(er);get baseUrl(){let{host:n,port:e}=this.settings.config();return`http://${n}:${e??9100}`}async getDefaultPrinter(){let n=await this.fetchFromAgent("/default?type=printer");if(!n.ok)return null;let e=await n.json();return this.toPrinterDevice(e)}async listAvailablePrinters(){let n=await this.fetchFromAgent("/available");return n.ok?((await n.json()).printer??[]).map(i=>this.toPrinterDevice(i)).filter(i=>i!==null):[]}async write(n,e){let i=await this.fetchFromAgent("/write",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({device:n,data:e})});if(!i.ok)throw new Hg(`O agente Zebra Browser Print recusou a impress\xE3o (HTTP ${i.status}).`)}async fetchFromAgent(n,e){try{return await fetch(`${this.baseUrl}${n}`,e)}catch(i){throw new ul("N\xE3o foi poss\xEDvel falar com o Zebra Browser Print. Ele precisa estar instalado e em execu\xE7\xE3o.",{cause:i})}}toPrinterDevice(n){return!n||!("name"in n)||!n.name?null:{name:n.name,uid:n.uid,connection:n.connection,deviceType:n.deviceType,version:n.version,provider:n.provider,manufacturer:n.manufacturer}}static \u0275fac=function(e){return new(e||t)};static \u0275prov=G({token:t,factory:t.\u0275fac,providedIn:"root"})};var Vu=class t{constructor(n){this.client=n}client;detectDefaultPrinter(){return this.client.getDefaultPrinter()}sendRaw(n,e){return this.client.write(n,e)}static \u0275fac=function(e){return new(e||t)(A(Lu))};static \u0275prov=G({token:t,factory:t.\u0275fac,providedIn:"root"})};var fl=class{_attachedHost=null;attach(n){return this._attachedHost=n,n.attach(this)}detach(){let n=this._attachedHost;n!=null&&(this._attachedHost=null,n.detach())}get isAttached(){return this._attachedHost!=null}setAttachedHost(n){this._attachedHost=n}},Cn=class extends fl{component;viewContainerRef;injector;projectableNodes;bindings;directives;constructor(n,e,i,r,o,s){super(),this.component=n,this.viewContainerRef=e,this.injector=i,this.projectableNodes=r,this.bindings=o||null,this.directives=s||null}},Dn=class extends fl{templateRef;viewContainerRef;context;injector;constructor(n,e,i,r){super(),this.templateRef=n,this.viewContainerRef=e,this.context=i,this.injector=r}get origin(){return this.templateRef.elementRef}attach(n,e=this.context){return this.context=e,super.attach(n)}detach(){return this.context=void 0,super.detach()}},Ug=class extends fl{element;constructor(n){super(),this.element=n instanceof O?n.nativeElement:n}},tr=class{_attachedPortal=null;_disposeFn=null;_isDisposed=!1;hasAttached(){return!!this._attachedPortal}attach(n){if(n instanceof Cn)return this._attachedPortal=n,this.attachComponentPortal(n);if(n instanceof Dn)return this._attachedPortal=n,this.attachTemplatePortal(n);if(this.attachDomPortal&&n instanceof Ug)return this._attachedPortal=n,this.attachDomPortal(n)}attachDomPortal=null;detach(){this._attachedPortal&&(this._attachedPortal.setAttachedHost(null),this._attachedPortal=null),this._invokeDisposeFn()}dispose(){this.hasAttached()&&this.detach(),this._invokeDisposeFn(),this._isDisposed=!0}setDisposeFn(n){this._disposeFn=n}_invokeDisposeFn(){this._disposeFn&&(this._disposeFn(),this._disposeFn=null)}},hl=class extends tr{outletElement;_appRef;_defaultInjector;constructor(n,e,i){super(),this.outletElement=n,this._appRef=e,this._defaultInjector=i}attachComponentPortal(n){let e;if(n.viewContainerRef){let i=n.injector||n.viewContainerRef.injector,r=i.get(Hn,null,{optional:!0})||void 0;e=n.viewContainerRef.createComponent(n.component,{index:n.viewContainerRef.length,injector:i,ngModuleRef:r,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0,directives:n.directives||void 0}),this.setDisposeFn(()=>e.destroy())}else{let i=this._appRef,r=n.injector||this._defaultInjector||R.NULL,o=r.get(Ie,i.injector);e=Zd(n.component,{elementInjector:r,environmentInjector:o,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0,directives:n.directives||void 0}),i.attachView(e.hostView),this.setDisposeFn(()=>{i.viewCount>0&&i.detachView(e.hostView),e.destroy()})}return this.outletElement.appendChild(this._getComponentRootNode(e)),this._attachedPortal=n,e}attachTemplatePortal(n){let e=n.viewContainerRef,i=e.createEmbeddedView(n.templateRef,n.context,{injector:n.injector});return i.rootNodes.forEach(r=>this.outletElement.appendChild(r)),i.detectChanges(),this.setDisposeFn(()=>{let r=e.indexOf(i);r!==-1&&e.remove(r)}),this._attachedPortal=n,i}attachDomPortal=n=>{let e=n.element;e.parentNode;let i=this.outletElement.ownerDocument.createComment("dom-portal");e.parentNode.insertBefore(i,e),this.outletElement.appendChild(e),this._attachedPortal=n,super.setDisposeFn(()=>{i.parentNode&&i.parentNode.replaceChild(e,i)})};dispose(){super.dispose(),this.outletElement.remove()}_getComponentRootNode(n){return n.hostView.rootNodes[0]}};var Gn=(()=>{class t extends tr{_moduleRef=d(Hn,{optional:!0});_document=d(L);_viewContainerRef=d(bt);_isInitialized=!1;_attachedRef=null;get portal(){return this._attachedPortal}set portal(e){this.hasAttached()&&!e&&!this._isInitialized||(this.hasAttached()&&super.detach(),e&&super.attach(e),this._attachedPortal=e||null)}attached=new W;get attachedRef(){return this._attachedRef}ngOnInit(){this._isInitialized=!0}ngOnDestroy(){super.dispose(),this._attachedRef=this._attachedPortal=null}attachComponentPortal(e){e.setAttachedHost(this);let i=e.viewContainerRef!=null?e.viewContainerRef:this._viewContainerRef,r=i.createComponent(e.component,{index:i.length,injector:e.injector||i.injector,projectableNodes:e.projectableNodes||void 0,ngModuleRef:this._moduleRef||void 0,bindings:e.bindings||void 0,directives:e.directives||void 0});return i!==this._viewContainerRef&&this._getRootNode().appendChild(r.hostView.rootNodes[0]),super.setDisposeFn(()=>r.destroy()),this._attachedPortal=e,this._attachedRef=r,this.attached.emit(r),r}attachTemplatePortal(e){e.setAttachedHost(this);let i=this._viewContainerRef.createEmbeddedView(e.templateRef,e.context,{injector:e.injector});return super.setDisposeFn(()=>this._viewContainerRef.clear()),this._attachedPortal=e,this._attachedRef=i,this.attached.emit(i),i}attachDomPortal=e=>{let i=e.element;i.parentNode;let r=this._document.createComment("dom-portal");e.setAttachedHost(this),i.parentNode.insertBefore(r,i),this._getRootNode().appendChild(i),this._attachedPortal=e,super.setDisposeFn(()=>{r.parentNode&&r.parentNode.replaceChild(i,r)})};_getRootNode(){let e=this._viewContainerRef.element.nativeElement;return e.nodeType===e.ELEMENT_NODE?e:e.parentNode}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ht(t)))(r||t)}})();static \u0275dir=H({type:t,selectors:[["","cdkPortalOutlet",""]],inputs:{portal:[0,"cdkPortalOutlet","portal"]},outputs:{attached:"attached"},exportAs:["cdkPortalOutlet"],features:[Me]})}return t})(),vi=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({})}return t})();function ro(t){return t.buttons===0||t.detail===0}function oo(t){let n=t.touches&&t.touches[0]||t.changedTouches&&t.changedTouches[0];return!!n&&n.identifier===-1&&(n.radiusX==null||n.radiusX===1)&&(n.radiusY==null||n.radiusY===1)}var zg;function iD(){if(zg==null){let t=typeof document<"u"?document.head:null;zg=!!(t&&(t.createShadowRoot||t.attachShadow))}return zg}function $g(t){if(iD()){let n=t.getRootNode?t.getRootNode():null;if(typeof ShadowRoot<"u"&&ShadowRoot&&n instanceof ShadowRoot)return n}return null}function ml(){let t=typeof document<"u"&&document?document.activeElement:null;for(;t&&t.shadowRoot;){let n=t.shadowRoot.activeElement;if(n===t)break;t=n}return t}function yt(t){if(t.composedPath)try{return t.composedPath()[0]}catch{}return t.target}var Gg;try{Gg=typeof Intl<"u"&&Intl.v8BreakIterator}catch{Gg=!1}var be=(()=>{class t{_platformId=d(Ur);isBrowser=this._platformId?M0(this._platformId):typeof document=="object"&&!!document;EDGE=this.isBrowser&&/(edge)/i.test(navigator.userAgent);TRIDENT=this.isBrowser&&/(msie|trident)/i.test(navigator.userAgent);BLINK=this.isBrowser&&!!(window.chrome||Gg)&&typeof CSS<"u"&&!this.EDGE&&!this.TRIDENT;WEBKIT=this.isBrowser&&/AppleWebKit/i.test(navigator.userAgent)&&!this.BLINK&&!this.EDGE&&!this.TRIDENT;IOS=this.isBrowser&&/iPad|iPhone|iPod/.test(navigator.userAgent)&&!("MSStream"in window);FIREFOX=this.isBrowser&&/(firefox|minefield)/i.test(navigator.userAgent);ANDROID=this.isBrowser&&/android/i.test(navigator.userAgent)&&!this.TRIDENT;SAFARI=this.isBrowser&&/safari/i.test(navigator.userAgent)&&this.WEBKIT;static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();var pl;function rD(){if(pl==null&&typeof window<"u")try{window.addEventListener("test",null,Object.defineProperty({},"passive",{get:()=>pl=!0}))}finally{pl=pl||!1}return pl}function hs(t){return rD()?t:!!t.capture}function bi(t,n=0){return oD(t)?Number(t):arguments.length===2?n:0}function oD(t){return!isNaN(parseFloat(t))&&!isNaN(Number(t))}function $t(t){return t instanceof O?t.nativeElement:t}var sD=new b("cdk-input-modality-detector-options"),aD={ignoreKeys:[18,17,224,91,16]},lD=650,Wg={passive:!0,capture:!0},cD=(()=>{class t{_platform=d(be);_listenerCleanups;modalityDetected;modalityChanged;get mostRecentModality(){return this._modality.value}_mostRecentTarget=null;_modality=new tt(null);_options;_lastTouchMs=0;_onKeydown=e=>{this._options?.ignoreKeys?.some(i=>i===e.keyCode)||(this._modality.next("keyboard"),this._mostRecentTarget=yt(e))};_onMousedown=e=>{Date.now()-this._lastTouchMs<lD||(this._modality.next(ro(e)?"keyboard":"mouse"),this._mostRecentTarget=yt(e))};_onTouchstart=e=>{if(oo(e)){this._modality.next("keyboard");return}this._lastTouchMs=Date.now(),this._modality.next("touch"),this._mostRecentTarget=yt(e)};constructor(){let e=d(N),i=d(L),r=d(sD,{optional:!0});if(this._options=v(v({},aD),r),this.modalityDetected=this._modality.pipe(Qs(1)),this.modalityChanged=this.modalityDetected.pipe(Ic()),this._platform.isBrowser){let o=d(Ke).createRenderer(null,null);this._listenerCleanups=e.runOutsideAngular(()=>[o.listen(i,"keydown",this._onKeydown,Wg),o.listen(i,"mousedown",this._onMousedown,Wg),o.listen(i,"touchstart",this._onTouchstart,Wg)])}}ngOnDestroy(){this._modality.complete(),this._listenerCleanups?.forEach(e=>e())}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})(),gl=(function(t){return t[t.IMMEDIATE=0]="IMMEDIATE",t[t.EVENTUAL=1]="EVENTUAL",t})(gl||{}),dD=new b("cdk-focus-monitor-default-options"),Bu=hs({passive:!0,capture:!0}),rn=(()=>{class t{_ngZone=d(N);_platform=d(be);_inputModalityDetector=d(cD);_origin=null;_lastFocusOrigin=null;_windowFocused=!1;_windowFocusTimeoutId;_originTimeoutId;_originFromTouchInteraction=!1;_elementInfo=new Map;_monitoredElementCount=0;_rootNodeFocusListenerCount=new Map;_detectionMode;_windowFocusListener=()=>{this._windowFocused=!0,this._windowFocusTimeoutId=setTimeout(()=>this._windowFocused=!1)};_document=d(L);_stopInputModalityDetector=new E;constructor(){let e=d(dD,{optional:!0});this._detectionMode=e?.detectionMode||gl.IMMEDIATE}_rootNodeFocusAndBlurListener=e=>{let i=yt(e);for(let r=i;r;r=r.parentElement)e.type==="focus"?this._onFocus(e,r):this._onBlur(e,r)};monitor(e,i=!1){let r=$t(e);if(!this._platform.isBrowser||r.nodeType!==1)return Y();let o=$g(r)||this._document,s=this._elementInfo.get(r);if(s)return i&&(s.checkChildren=!0),s.subject;let a={checkChildren:i,subject:new E,rootNode:o};return this._elementInfo.set(r,a),this._registerGlobalListeners(a),a.subject}stopMonitoring(e){let i=$t(e),r=this._elementInfo.get(i);r&&(r.subject.complete(),this._setClasses(i),this._elementInfo.delete(i),this._removeGlobalListeners(r))}focusVia(e,i,r){let o=$t(e),s=this._document.activeElement;o===s?this._getClosestElementsInfo(o).forEach(([a,l])=>this._originChanged(a,i,l)):(this._setOrigin(i),typeof o.focus=="function"&&o.focus(r))}ngOnDestroy(){this._elementInfo.forEach((e,i)=>this.stopMonitoring(i))}_getWindow(){return this._document.defaultView||window}_getFocusOrigin(e){return this._origin?this._originFromTouchInteraction?this._shouldBeAttributedToTouch(e)?"touch":"program":this._origin:this._windowFocused&&this._lastFocusOrigin?this._lastFocusOrigin:e&&this._isLastInteractionFromInputLabel(e)?"mouse":"program"}_shouldBeAttributedToTouch(e){return this._detectionMode===gl.EVENTUAL||!!e?.contains(this._inputModalityDetector._mostRecentTarget)}_setClasses(e,i){e.classList.toggle("cdk-focused",!!i),e.classList.toggle("cdk-touch-focused",i==="touch"),e.classList.toggle("cdk-keyboard-focused",i==="keyboard"),e.classList.toggle("cdk-mouse-focused",i==="mouse"),e.classList.toggle("cdk-program-focused",i==="program")}_setOrigin(e,i=!1){this._ngZone.runOutsideAngular(()=>{if(this._origin=e,this._originFromTouchInteraction=e==="touch"&&i,this._detectionMode===gl.IMMEDIATE){clearTimeout(this._originTimeoutId);let r=this._originFromTouchInteraction?lD:1;this._originTimeoutId=setTimeout(()=>this._origin=null,r)}})}_onFocus(e,i){let r=this._elementInfo.get(i),o=yt(e);!r||!r.checkChildren&&i!==o||this._originChanged(i,this._getFocusOrigin(o),r)}_onBlur(e,i){let r=this._elementInfo.get(i);!r||r.checkChildren&&e.relatedTarget instanceof Node&&i.contains(e.relatedTarget)||(this._setClasses(i),this._emitOrigin(r,null))}_emitOrigin(e,i){e.subject.observers.length&&this._ngZone.run(()=>e.subject.next(i))}_registerGlobalListeners(e){if(!this._platform.isBrowser)return;let i=e.rootNode,r=this._rootNodeFocusListenerCount.get(i)||0;r||this._ngZone.runOutsideAngular(()=>{i.addEventListener("focus",this._rootNodeFocusAndBlurListener,Bu),i.addEventListener("blur",this._rootNodeFocusAndBlurListener,Bu)}),this._rootNodeFocusListenerCount.set(i,r+1),++this._monitoredElementCount===1&&(this._ngZone.runOutsideAngular(()=>{this._getWindow().addEventListener("focus",this._windowFocusListener)}),this._inputModalityDetector.modalityDetected.pipe(ye(this._stopInputModalityDetector)).subscribe(o=>{this._setOrigin(o,!0)}))}_removeGlobalListeners(e){let i=e.rootNode;if(this._rootNodeFocusListenerCount.has(i)){let r=this._rootNodeFocusListenerCount.get(i);r>1?this._rootNodeFocusListenerCount.set(i,r-1):(i.removeEventListener("focus",this._rootNodeFocusAndBlurListener,Bu),i.removeEventListener("blur",this._rootNodeFocusAndBlurListener,Bu),this._rootNodeFocusListenerCount.delete(i))}--this._monitoredElementCount||(this._getWindow().removeEventListener("focus",this._windowFocusListener),this._stopInputModalityDetector.next(),clearTimeout(this._windowFocusTimeoutId),clearTimeout(this._originTimeoutId))}_originChanged(e,i,r){this._setClasses(e,i),this._emitOrigin(r,i),this._lastFocusOrigin=i}_getClosestElementsInfo(e){let i=[];return this._elementInfo.forEach((r,o)=>{(o===e||r.checkChildren&&o.contains(e))&&i.push([o,r])}),i}_isLastInteractionFromInputLabel(e){let{_mostRecentTarget:i,mostRecentModality:r}=this._inputModalityDetector;if(r!=="mouse"||!i||i===e||e.nodeName!=="INPUT"&&e.nodeName!=="TEXTAREA"||e.disabled)return!1;let o=e.labels;if(o){for(let s=0;s<o.length;s++)if(o[s].contains(i))return!0}return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();var ju=new WeakMap,ze=(()=>{class t{_appRef;_injector=d(R);_environmentInjector=d(Ie);load(e){let i=this._appRef=this._appRef||this._injector.get(Ft),r=ju.get(i);r||(r={loaders:new Set,refs:[]},ju.set(i,r),i.onDestroy(()=>{ju.get(i)?.refs.forEach(o=>o.destroy()),ju.delete(i)})),r.loaders.has(e)||(r.loaders.add(e),r.refs.push(Zd(e,{environmentInjector:this._environmentInjector})))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();var ms=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["ng-component"]],exportAs:["cdkVisuallyHidden"],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-visually-hidden {
  border: 0;
  clip: rect(0 0 0 0);
  height: 1px;
  margin: -1px;
  overflow: hidden;
  padding: 0;
  position: absolute;
  width: 1px;
  white-space: nowrap;
  outline: 0;
  -webkit-appearance: none;
  -moz-appearance: none;
  left: 0;
}
[dir=rtl] .cdk-visually-hidden {
  left: auto;
  right: 0;
}
`],encapsulation:2})}return t})(),Hu;function VO(){if(Hu===void 0&&(Hu=null,typeof window<"u")){let t=window;if(t.trustedTypes!==void 0)try{Hu=t.trustedTypes.createPolicy("angular#components",{createHTML:n=>n})}catch(n){console.error(n)}}return Hu}function so(t){return VO()?.createHTML(t)||t}function uD(t,n,e){let i=e.sanitize(st.HTML,n);t.innerHTML=so(i||"")}function ps(t){return Array.isArray(t)?t:[t]}var fD=new Set,ao,gs=(()=>{class t{_platform=d(be);_nonce=d(Li,{optional:!0});_matchMedia;constructor(){this._matchMedia=this._platform.isBrowser&&window.matchMedia?window.matchMedia.bind(window):jO}matchMedia(e){return(this._platform.WEBKIT||this._platform.BLINK)&&BO(e,this._nonce),this._matchMedia(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();function BO(t,n){if(!fD.has(t))try{ao||(ao=document.createElement("style"),n&&ao.setAttribute("nonce",n),ao.setAttribute("type","text/css"),document.head.appendChild(ao)),ao.sheet&&(ao.sheet.insertRule(`@media ${t.replace(/[{}]/g,"")} {body{ }}`,0),fD.add(t))}catch(e){console.error(e)}}function jO(t){return{matches:t==="all"||t==="",media:t,addListener:()=>{},removeListener:()=>{}}}var lo=(()=>{class t{_mediaMatcher=d(gs);_zone=d(N);_queries=new Map;_destroySubject=new E;ngOnDestroy(){this._destroySubject.next(),this._destroySubject.complete()}isMatched(e){return hD(ps(e)).some(r=>this._registerQuery(r).mql.matches)}observe(e){let r=hD(ps(e)).map(s=>this._registerQuery(s).observable),o=Ys(r);return o=Ni(o.pipe(Ae(1)),o.pipe(Qs(1),Xs(0))),o.pipe(oe(s=>{let a={matches:!1,breakpoints:{}};return s.forEach(({matches:l,query:c})=>{a.matches=a.matches||l,a.breakpoints[c]=l}),a}))}_registerQuery(e){if(this._queries.has(e))return this._queries.get(e);let i=this._mediaMatcher.matchMedia(e),o={observable:new ne(s=>{let a=l=>this._zone.run(()=>s.next(l));return i.addListener(a),()=>{i.removeListener(a)}}).pipe(nt(i),oe(({matches:s})=>({query:e,matches:s})),ye(this._destroySubject)),mql:i};return this._queries.set(e,o),o}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();function hD(t){return t.map(n=>n.split(",")).reduce((n,e)=>n.concat(e)).map(n=>n.trim())}var HO=(()=>{class t{create(e){return typeof MutationObserver>"u"?null:new MutationObserver(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();var Uu=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({providers:[HO]})}return t})();var Zg=(()=>{class t{_platform=d(be);isDisabled(e){return e.hasAttribute("disabled")}isVisible(e){return zO(e)&&getComputedStyle(e).visibility==="visible"}isTabbable(e){if(!this._platform.isBrowser)return!1;let i=UO(KO(e));if(i&&(mD(i)===-1||!this.isVisible(i)))return!1;let r=e.nodeName.toLowerCase(),o=mD(e);return e.hasAttribute("contenteditable")?o!==-1:r==="iframe"||r==="object"||this._platform.WEBKIT&&this._platform.IOS&&!ZO(e)?!1:r==="audio"?e.hasAttribute("controls")?o!==-1:!1:r==="video"?o===-1?!1:o!==null?!0:this._platform.FIREFOX||e.hasAttribute("controls"):e.tabIndex>=0}isFocusable(e,i){return XO(e)&&!this.isDisabled(e)&&(i?.ignoreVisibility||this.isVisible(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();function UO(t){try{return t.frameElement}catch{return null}}function zO(t){return!!(t.offsetWidth||t.offsetHeight||typeof t.getClientRects=="function"&&t.getClientRects().length)}function $O(t){let n=t.nodeName.toLowerCase();return n==="input"||n==="select"||n==="button"||n==="textarea"}function GO(t){return qO(t)&&t.type=="hidden"}function WO(t){return YO(t)&&t.hasAttribute("href")}function qO(t){return t.nodeName.toLowerCase()=="input"}function YO(t){return t.nodeName.toLowerCase()=="a"}function vD(t){if(!t.hasAttribute("tabindex")||t.tabIndex===void 0)return!1;let n=t.getAttribute("tabindex");return!!(n&&!isNaN(parseInt(n,10)))}function mD(t){if(!vD(t))return null;let n=parseInt(t.getAttribute("tabindex")||"",10);return isNaN(n)?-1:n}function ZO(t){let n=t.nodeName.toLowerCase(),e=n==="input"&&t.type;return e==="text"||e==="password"||n==="select"||n==="textarea"}function XO(t){return GO(t)?!1:$O(t)||WO(t)||t.hasAttribute("contenteditable")||vD(t)}function KO(t){return t.ownerDocument&&t.ownerDocument.defaultView||window}var Yg=class{_element;_checker;_ngZone;_document;_injector;_startAnchor=null;_endAnchor=null;_hasAttached=!1;startAnchorListener=()=>{!this.focusLastTabbableElement()&&this._checker.isFocusable(this._element)&&this._element.focus()};endAnchorListener=()=>{!this.focusFirstTabbableElement()&&this._checker.isFocusable(this._element)&&this._element.focus()};get enabled(){return this._enabled}set enabled(n){this._enabled=n,this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(n,this._startAnchor),this._toggleAnchorTabIndex(n,this._endAnchor))}_enabled=!0;constructor(n,e,i,r,o=!1,s){this._element=n,this._checker=e,this._ngZone=i,this._document=r,this._injector=s,o||this.attachAnchors()}destroy(){let n=this._startAnchor,e=this._endAnchor;n&&(n.removeEventListener("focus",this.startAnchorListener),n.remove()),e&&(e.removeEventListener("focus",this.endAnchorListener),e.remove()),this._startAnchor=this._endAnchor=null,this._hasAttached=!1}attachAnchors(){return this._hasAttached?!0:(this._ngZone.runOutsideAngular(()=>{this._startAnchor||(this._startAnchor=this._createAnchor(),this._startAnchor.addEventListener("focus",this.startAnchorListener)),this._endAnchor||(this._endAnchor=this._createAnchor(),this._endAnchor.addEventListener("focus",this.endAnchorListener))}),this._element.parentNode&&(this._element.parentNode.insertBefore(this._startAnchor,this._element),this._element.parentNode.insertBefore(this._endAnchor,this._element.nextSibling),this._hasAttached=!0),this._hasAttached)}focusInitialElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusInitialElement(n)))})}focusFirstTabbableElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusFirstTabbableElement(n)))})}focusLastTabbableElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusLastTabbableElement(n)))})}_getRegionBoundary(n){let e=this._element.querySelectorAll(`[cdk-focus-region-${n}], [cdkFocusRegion${n}], [cdk-focus-${n}]`);return n=="start"?e.length?e[0]:this._getFirstTabbableElement(this._element):e.length?e[e.length-1]:this._getLastTabbableElement(this._element)}focusInitialElement(n){let e=this._element.querySelector("[cdk-focus-initial], [cdkFocusInitial]");if(e){if(!this._checker.isFocusable(e)){let i=this._getFirstTabbableElement(e);return i?.focus(n),!!i}return e.focus(n),!0}return this.focusFirstTabbableElement(n)}focusFirstTabbableElement(n){let e=this._getRegionBoundary("start");return e&&e.focus(n),!!e}focusLastTabbableElement(n){let e=this._getRegionBoundary("end");return e&&e.focus(n),!!e}hasAttached(){return this._hasAttached}_getFirstTabbableElement(n){if(this._checker.isFocusable(n)&&this._checker.isTabbable(n))return n;let e=n.children;for(let i=0;i<e.length;i++){let r=e[i].nodeType===this._document.ELEMENT_NODE?this._getFirstTabbableElement(e[i]):null;if(r)return r}return null}_getLastTabbableElement(n){if(this._checker.isFocusable(n)&&this._checker.isTabbable(n))return n;let e=n.children;for(let i=e.length-1;i>=0;i--){let r=e[i].nodeType===this._document.ELEMENT_NODE?this._getLastTabbableElement(e[i]):null;if(r)return r}return null}_createAnchor(){let n=this._document.createElement("div");return this._toggleAnchorTabIndex(this._enabled,n),n.classList.add("cdk-visually-hidden"),n.classList.add("cdk-focus-trap-anchor"),n.setAttribute("aria-hidden","true"),n}_toggleAnchorTabIndex(n,e){n?e.setAttribute("tabindex","0"):e.removeAttribute("tabindex")}toggleAnchors(n){this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(n,this._startAnchor),this._toggleAnchorTabIndex(n,this._endAnchor))}_executeOnStable(n){Qe(n,{injector:this._injector})}},Xg=(()=>{class t{_checker=d(Zg);_ngZone=d(N);_document=d(L);_injector=d(R);constructor(){d(ze).load(ms)}create(e,i=!1){return new Yg(e,this._checker,this._ngZone,this._document,i,this._injector)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();var bD=new b("liveAnnouncerElement",{providedIn:"root",factory:()=>null}),_D=new b("LIVE_ANNOUNCER_DEFAULT_OPTIONS"),QO=0,vl=(()=>{class t{_ngZone=d(N);_defaultOptions=d(_D,{optional:!0});_liveElement;_document=d(L);_sanitizer=d(za);_previousTimeout;_currentPromise;_currentResolve;constructor(){let e=d(bD,{optional:!0});this._liveElement=e||this._createLiveElement()}announce(e,...i){let r=this._defaultOptions,o,s;return i.length===1&&typeof i[0]=="number"?s=i[0]:[o,s]=i,this.clear(),clearTimeout(this._previousTimeout),o||(o=r&&r.politeness?r.politeness:"polite"),s==null&&r&&(s=r.duration),this._liveElement.setAttribute("aria-live",o),this._liveElement.id&&this._exposeAnnouncerToModals(this._liveElement.id),this._ngZone.runOutsideAngular(()=>(this._currentPromise||(this._currentPromise=new Promise(a=>this._currentResolve=a)),clearTimeout(this._previousTimeout),this._previousTimeout=setTimeout(()=>{!e||typeof e=="string"?this._liveElement.textContent=e:uD(this._liveElement,e,this._sanitizer),typeof s=="number"&&(this._previousTimeout=setTimeout(()=>this.clear(),s)),this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0},100),this._currentPromise))}clear(){this._liveElement&&(this._liveElement.textContent="")}ngOnDestroy(){clearTimeout(this._previousTimeout),this._liveElement?.remove(),this._liveElement=null,this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0}_createLiveElement(){let e="cdk-live-announcer-element",i=this._document.getElementsByClassName(e),r=this._document.createElement("div");for(let o=0;o<i.length;o++)i[o].remove();return r.classList.add(e),r.classList.add("cdk-visually-hidden"),r.setAttribute("aria-atomic","true"),r.setAttribute("aria-live","polite"),r.id=`cdk-live-announcer-${QO++}`,this._document.body.appendChild(r),r}_exposeAnnouncerToModals(e){let i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],s=o.getAttribute("aria-owns");s?s.indexOf(e)===-1&&o.setAttribute("aria-owns",s+" "+e):o.setAttribute("aria-owns",e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();var nr=(function(t){return t[t.NONE=0]="NONE",t[t.BLACK_ON_WHITE=1]="BLACK_ON_WHITE",t[t.WHITE_ON_BLACK=2]="WHITE_ON_BLACK",t})(nr||{}),pD="cdk-high-contrast-black-on-white",gD="cdk-high-contrast-white-on-black",qg="cdk-high-contrast-active",yD=(()=>{class t{_platform=d(be);_hasCheckedHighContrastMode=!1;_document=d(L);_breakpointSubscription;constructor(){this._breakpointSubscription=d(lo).observe("(forced-colors: active)").subscribe(()=>{this._hasCheckedHighContrastMode&&(this._hasCheckedHighContrastMode=!1,this._applyBodyHighContrastModeCssClasses())})}getHighContrastMode(){if(!this._platform.isBrowser)return nr.NONE;let e=this._document.createElement("div");e.style.backgroundColor="rgb(1,2,3)",e.style.position="absolute",this._document.body.appendChild(e);let i=this._document.defaultView||window,r=i&&i.getComputedStyle?i.getComputedStyle(e):null,o=(r&&r.backgroundColor||"").replace(/ /g,"");switch(e.remove(),o){case"rgb(0,0,0)":case"rgb(45,50,54)":case"rgb(32,32,32)":return nr.WHITE_ON_BLACK;case"rgb(255,255,255)":case"rgb(255,250,239)":return nr.BLACK_ON_WHITE}return nr.NONE}ngOnDestroy(){this._breakpointSubscription.unsubscribe()}_applyBodyHighContrastModeCssClasses(){if(!this._hasCheckedHighContrastMode&&this._platform.isBrowser&&this._document.body){let e=this._document.body.classList;e.remove(qg,pD,gD),this._hasCheckedHighContrastMode=!0;let i=this.getHighContrastMode();i===nr.BLACK_ON_WHITE?e.add(qg,pD):i===nr.WHITE_ON_BLACK&&e.add(qg,gD)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})(),bl=(()=>{class t{constructor(){d(yD)._applyBodyHighContrastModeCssClasses()}static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({imports:[Uu]})}return t})();function Kg(){return typeof __karma__<"u"&&!!__karma__||typeof jasmine<"u"&&!!jasmine||typeof jest<"u"&&!!jest||typeof Mocha<"u"&&!!Mocha}function Ze(t){return t==null?"":typeof t=="string"?t:`${t}px`}var JO=new b("cdk-dir-doc",{providedIn:"root",factory:()=>d(L)}),eP=/^(ar|ckb|dv|he|iw|fa|nqo|ps|sd|ug|ur|yi|.*[-_](Adlm|Arab|Hebr|Nkoo|Rohg|Thaa))(?!.*[-_](Latn|Cyrl)($|-|_))($|-|_)/i;function SD(t){let n=t?.toLowerCase()||"";return n==="auto"&&typeof navigator<"u"&&navigator?.language?eP.test(navigator.language)?"rtl":"ltr":n==="rtl"?"rtl":"ltr"}var ct=(()=>{class t{get value(){return this.valueSignal()}valueSignal=j("ltr");change=new W;constructor(){let e=d(JO,{optional:!0});if(e){let i=e.body?e.body.dir:null,r=e.documentElement?e.documentElement.dir:null;this.valueSignal.set(SD(i||r||"ltr"))}}ngOnDestroy(){this.change.complete()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();var En=(function(t){return t[t.NORMAL=0]="NORMAL",t[t.NEGATED=1]="NEGATED",t[t.INVERTED=2]="INVERTED",t})(En||{}),zu,co;function $u(){if(co==null){if(typeof document!="object"||!document||typeof Element!="function"||!Element)return co=!1,co;if(document.documentElement?.style&&"scrollBehavior"in document.documentElement.style)co=!0;else{let t=Element.prototype.scrollTo;t?co=!/\{\s*\[native code\]\s*\}/.test(t.toString()):co=!1}}return co}function vs(){if(typeof document!="object"||!document)return En.NORMAL;if(zu==null){let t=document.createElement("div"),n=t.style;t.dir="rtl",n.width="1px",n.overflow="auto",n.visibility="hidden",n.pointerEvents="none",n.position="absolute";let e=document.createElement("div"),i=e.style;i.width="2px",i.height="1px",t.appendChild(e),document.body.appendChild(t),zu=En.NORMAL,t.scrollLeft===0&&(t.scrollLeft=1,zu=t.scrollLeft===0?En.NEGATED:En.INVERTED),t.remove()}return zu}var ve=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({})}return t})();var tP=20,ir=(()=>{class t{_ngZone=d(N);_platform=d(be);_renderer=d(Ke).createRenderer(null,null);_cleanupGlobalListener;_scrolled=new E;_scrolledCount=0;scrollContainers=new Map;register(e){this.scrollContainers.has(e)||this.scrollContainers.set(e,e.elementScrolled().subscribe(()=>this._scrolled.next(e)))}deregister(e){let i=this.scrollContainers.get(e);i&&(i.unsubscribe(),this.scrollContainers.delete(e))}scrolled(e=tP){return this._platform.isBrowser?new ne(i=>{this._cleanupGlobalListener||(this._cleanupGlobalListener=this._ngZone.runOutsideAngular(()=>this._renderer.listen("document","scroll",()=>this._scrolled.next())));let r=e>0?this._scrolled.pipe(xc(e)).subscribe(i):this._scrolled.subscribe(i);return this._scrolledCount++,()=>{r.unsubscribe(),this._scrolledCount--,this._scrolledCount||(this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0)}}):Y()}ngOnDestroy(){this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0,this.scrollContainers.forEach((e,i)=>this.deregister(i)),this._scrolled.complete()}ancestorScrolled(e,i){let r=this.getAncestorScrollContainers(e);return this.scrolled(i).pipe(ge(o=>!o||r.indexOf(o)>-1))}getAncestorScrollContainers(e){let i=[];return this.scrollContainers.forEach((r,o)=>{this._targetContainsElement(o,e)&&i.push(o)}),i}_targetContainsElement(e,i){let r=$t(i),o=e.getElementRef().nativeElement;do if(r==o)return!0;while(r=r.parentElement);return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})(),Qg=(()=>{class t{elementRef=d(O);scrollDispatcher=d(ir);ngZone=d(N);dir=d(ct,{optional:!0});_scrollElement=this.elementRef.nativeElement;_destroyed=new E;_renderer=d(Re);_cleanupScroll;_elementScrolled=new E;ngOnInit(){this._cleanupScroll=this.ngZone.runOutsideAngular(()=>this._renderer.listen(this._scrollElement,"scroll",e=>this._elementScrolled.next(e))),this.scrollDispatcher.register(this)}ngOnDestroy(){this._cleanupScroll?.(),this._elementScrolled.complete(),this.scrollDispatcher.deregister(this),this._destroyed.next(),this._destroyed.complete()}elementScrolled(){return this._elementScrolled}getElementRef(){return this.elementRef}scrollTo(e){let i=this.elementRef.nativeElement,r=this.dir&&this.dir.value=="rtl";e.left==null&&(e.left=r?e.end:e.start),e.right==null&&(e.right=r?e.start:e.end),e.bottom!=null&&(e.top=i.scrollHeight-i.clientHeight-e.bottom),r&&vs()!=En.NORMAL?(e.left!=null&&(e.right=i.scrollWidth-i.clientWidth-e.left),vs()==En.INVERTED?e.left=e.right:vs()==En.NEGATED&&(e.left=e.right?-e.right:e.right)):e.right!=null&&(e.left=i.scrollWidth-i.clientWidth-e.right),this._applyScrollToOptions(e)}_applyScrollToOptions(e){let i=this.elementRef.nativeElement;$u()?i.scrollTo(e):(e.top!=null&&(i.scrollTop=e.top),e.left!=null&&(i.scrollLeft=e.left))}measureScrollOffset(e){let i="left",r="right",o=this.elementRef.nativeElement;if(e=="top")return o.scrollTop;if(e=="bottom")return o.scrollHeight-o.clientHeight-o.scrollTop;let s=this.dir&&this.dir.value=="rtl";return e=="start"?e=s?r:i:e=="end"&&(e=s?i:r),s&&vs()==En.INVERTED?e==i?o.scrollWidth-o.clientWidth-o.scrollLeft:o.scrollLeft:s&&vs()==En.NEGATED?e==i?o.scrollLeft+o.scrollWidth-o.clientWidth:-o.scrollLeft:e==i?o.scrollLeft:o.scrollWidth-o.clientWidth-o.scrollLeft}static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,selectors:[["","cdk-scrollable",""],["","cdkScrollable",""]]})}return t})(),nP=20,yi=(()=>{class t{_platform=d(be);_listeners;_viewportSize=null;_change=new E;_document=d(L);constructor(){let e=d(N),i=d(Ke).createRenderer(null,null);e.runOutsideAngular(()=>{if(this._platform.isBrowser){let r=o=>this._change.next(o);this._listeners=[i.listen("window","resize",r),i.listen("window","orientationchange",r)]}this.change().subscribe(()=>this._viewportSize=null)})}ngOnDestroy(){this._listeners?.forEach(e=>e()),this._change.complete()}getViewportSize(){this._viewportSize||this._updateViewportSize();let e={width:this._viewportSize.width,height:this._viewportSize.height};return this._platform.isBrowser||(this._viewportSize=null),e}getViewportRect(){let e=this.getViewportScrollPosition(),{width:i,height:r}=this.getViewportSize();return{top:e.top,left:e.left,bottom:e.top+r,right:e.left+i,height:r,width:i}}getViewportScrollPosition(){if(!this._platform.isBrowser)return{top:0,left:0};let e=this._document,i=this._getWindow(),r=e.documentElement,o=r.getBoundingClientRect(),s=-o.top||e.body?.scrollTop||i.scrollY||r.scrollTop||0,a=-o.left||e.body?.scrollLeft||i.scrollX||r.scrollLeft||0;return{top:s,left:a}}change(e=nP){return e>0?this._change.pipe(xc(e)):this._change}_getWindow(){return this._document.defaultView||window}_updateViewportSize(){let e=this._getWindow();this._viewportSize=this._platform.isBrowser?{width:e.innerWidth,height:e.innerHeight}:{width:0,height:0}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();var _i=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({})}return t})(),Jg=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({imports:[ve,_i,ve,_i]})}return t})();var CD=new Map,xe=class t{_appId=d(si);static _infix=`a${Math.floor(Math.random()*1e5).toString()}`;getId(n,e=!1){this._appId!=="ng"&&(n+=this._appId);let i=CD.get(n);return i===void 0?i=0:i++,CD.set(n,i),`${n}${e?t._infix+"-":""}${i}`}static \u0275fac=function(e){return new(e||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})};function $e(t,...n){return n.length?n.some(e=>t[e]):t.altKey||t.shiftKey||t.ctrlKey||t.metaKey}var DD=$u();function rr(t){return new Gu(t.get(yi),t.get(L))}var Gu=class{_viewportRuler;_previousHTMLStyles={top:"",left:""};_previousScrollPosition;_isEnabled=!1;_document;constructor(n,e){this._viewportRuler=n,this._document=e}attach(){}enable(){if(this._canBeEnabled()){let n=this._document.documentElement;this._previousScrollPosition=this._viewportRuler.getViewportScrollPosition(),this._previousHTMLStyles.left=n.style.left||"",this._previousHTMLStyles.top=n.style.top||"",n.style.left=Ze(-this._previousScrollPosition.left),n.style.top=Ze(-this._previousScrollPosition.top),n.classList.add("cdk-global-scrollblock"),this._isEnabled=!0}}disable(){if(this._isEnabled){let n=this._document.documentElement,e=this._document.body,i=n.style,r=e.style,o=i.scrollBehavior||"",s=r.scrollBehavior||"";this._isEnabled=!1,i.left=this._previousHTMLStyles.left,i.top=this._previousHTMLStyles.top,n.classList.remove("cdk-global-scrollblock"),DD&&(i.scrollBehavior=r.scrollBehavior="auto"),window.scroll(this._previousScrollPosition.left,this._previousScrollPosition.top),DD&&(i.scrollBehavior=o,r.scrollBehavior=s)}}_canBeEnabled(){if(this._document.documentElement.classList.contains("cdk-global-scrollblock")||this._isEnabled)return!1;let e=this._document.documentElement,i=this._viewportRuler.getViewportSize();return e.scrollHeight>i.height||e.scrollWidth>i.width}};function ND(t,n){return new Wu(t.get(ir),t.get(N),t.get(yi),n)}var Wu=class{_scrollDispatcher;_ngZone;_viewportRuler;_config;_scrollSubscription=null;_overlayRef;_initialScrollPosition;constructor(n,e,i,r){this._scrollDispatcher=n,this._ngZone=e,this._viewportRuler=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(this._scrollSubscription)return;let n=this._scrollDispatcher.scrolled(0).pipe(ge(e=>!e||!this._overlayRef.overlayElement.contains(e.getElementRef().nativeElement)));this._config&&this._config.threshold&&this._config.threshold>1?(this._initialScrollPosition=this._viewportRuler.getViewportScrollPosition().top,this._scrollSubscription=n.subscribe(()=>{let e=this._viewportRuler.getViewportScrollPosition().top;Math.abs(e-this._initialScrollPosition)>this._config.threshold?this._detach():this._overlayRef.updatePosition()})):this._scrollSubscription=n.subscribe(this._detach)}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}_detach=()=>{this.disable(),this._overlayRef.hasAttached()&&this._ngZone.run(()=>this._overlayRef.detach())}};var _l=class{enable(){}disable(){}attach(){}};function tv(t,n){return n.some(e=>{let i=t.bottom<e.top,r=t.top>e.bottom,o=t.right<e.left,s=t.left>e.right;return i||r||o||s})}function ED(t,n){return n.some(e=>{let i=t.top<e.top,r=t.bottom>e.bottom,o=t.left<e.left,s=t.right>e.right;return i||r||o||s})}function Si(t,n){return new qu(t.get(ir),t.get(yi),t.get(N),n)}var qu=class{_scrollDispatcher;_viewportRuler;_ngZone;_config;_scrollSubscription=null;_overlayRef;constructor(n,e,i,r){this._scrollDispatcher=n,this._viewportRuler=e,this._ngZone=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(!this._scrollSubscription){let n=this._config?this._config.scrollThrottle:0;this._scrollSubscription=this._scrollDispatcher.scrolled(n).subscribe(()=>{if(this._overlayRef.updatePosition(),this._config&&this._config.autoClose){let e=this._overlayRef.overlayElement.getBoundingClientRect(),{width:i,height:r}=this._viewportRuler.getViewportSize();tv(e,[{width:i,height:r,bottom:r,right:i,top:0,left:0}])&&(this.disable(),this._ngZone.run(()=>this._overlayRef.detach()))}})}}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}},kD=(()=>{class t{_injector=d(R);noop=()=>new _l;close=e=>ND(this._injector,e);block=()=>rr(this._injector);reposition=e=>Si(this._injector,e);static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})(),wn=class{positionStrategy;scrollStrategy=new _l;panelClass="";hasBackdrop=!1;backdropClass="cdk-overlay-dark-backdrop";disableAnimations;width;height;minWidth;minHeight;maxWidth;maxHeight;direction;disposeOnNavigation=!1;usePopover;eventPredicate;constructor(n){if(n){let e=Object.keys(n);for(let i of e)n[i]!==void 0&&(this[i]=n[i])}}};var Yu=class{connectionPair;scrollableViewProperties;constructor(n,e){this.connectionPair=n,this.scrollableViewProperties=e}};var AD=(()=>{class t{_attachedOverlays=[];_document=d(L);_isAttached=!1;ngOnDestroy(){this.detach()}add(e){this.remove(e),this._attachedOverlays.push(e)}remove(e){let i=this._attachedOverlays.indexOf(e);i>-1&&this._attachedOverlays.splice(i,1),this._attachedOverlays.length===0&&this.detach()}canReceiveEvent(e,i,r){return r.observers.length<1?!1:e.eventPredicate?e.eventPredicate(i):!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})(),RD=(()=>{class t extends AD{_ngZone=d(N);_renderer=d(Ke).createRenderer(null,null);_cleanupKeydown;add(e){super.add(e),this._isAttached||(this._ngZone.runOutsideAngular(()=>{this._cleanupKeydown=this._renderer.listen("body","keydown",this._keydownListener)}),this._isAttached=!0)}detach(){this._isAttached&&(this._cleanupKeydown?.(),this._isAttached=!1)}_keydownListener=e=>{let i=this._attachedOverlays;for(let r=i.length-1;r>-1;r--){let o=i[r];if(this.canReceiveEvent(o,e,o._keydownEvents)){this._ngZone.run(()=>o._keydownEvents.next(e));break}}};static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})(),OD=(()=>{class t extends AD{_platform=d(be);_ngZone=d(N);_renderer=d(Ke).createRenderer(null,null);_cursorOriginalValue;_cursorStyleIsSet=!1;_pointerDownEventTarget=null;_cleanups;add(e){if(super.add(e),!this._isAttached){let i=this._document.body,r={capture:!0},o=this._renderer;this._cleanups=this._ngZone.runOutsideAngular(()=>[o.listen(i,"pointerdown",this._pointerDownListener,r),o.listen(i,"click",this._clickListener,r),o.listen(i,"auxclick",this._clickListener,r),o.listen(i,"contextmenu",this._clickListener,r)]),this._platform.IOS&&!this._cursorStyleIsSet&&(this._cursorOriginalValue=i.style.cursor,i.style.cursor="pointer",this._cursorStyleIsSet=!0),this._isAttached=!0}}detach(){this._isAttached&&(this._cleanups?.forEach(e=>e()),this._cleanups=void 0,this._platform.IOS&&this._cursorStyleIsSet&&(this._document.body.style.cursor=this._cursorOriginalValue,this._cursorStyleIsSet=!1),this._isAttached=!1)}_pointerDownListener=e=>{this._pointerDownEventTarget=yt(e)};_clickListener=e=>{let i=yt(e),r=e.type==="click"&&this._pointerDownEventTarget?this._pointerDownEventTarget:i;this._pointerDownEventTarget=null;let o=this._attachedOverlays.slice();for(let s=o.length-1;s>-1;s--){let a=o[s],l=a._outsidePointerEvents;if(!(!a.hasAttached()||!this.canReceiveEvent(a,e,l))){if(wD(a.overlayElement,i)||wD(a.overlayElement,r))break;this._ngZone?this._ngZone.run(()=>l.next(e)):l.next(e)}}};static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();function wD(t,n){let e=typeof ShadowRoot<"u"&&ShadowRoot,i=n;for(;i;){if(i===t)return!0;i=e&&i instanceof ShadowRoot?i.host:i.parentNode}return!1}var PD=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-overlay-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-overlay-container, .cdk-global-overlay-wrapper {
  pointer-events: none;
  top: 0;
  left: 0;
  height: 100%;
  width: 100%;
}

.cdk-overlay-container {
  position: fixed;
}
@layer cdk-overlay {
  .cdk-overlay-container {
    z-index: 1000;
  }
}
.cdk-overlay-container:empty {
  display: none;
}

.cdk-global-overlay-wrapper {
  display: flex;
  position: absolute;
}
@layer cdk-overlay {
  .cdk-global-overlay-wrapper {
    z-index: 1000;
  }
}

.cdk-overlay-pane {
  position: absolute;
  pointer-events: auto;
  box-sizing: border-box;
  display: flex;
  max-width: 100%;
  max-height: 100%;
}
@layer cdk-overlay {
  .cdk-overlay-pane {
    z-index: 1000;
  }
}

.cdk-overlay-backdrop {
  position: absolute;
  top: 0;
  bottom: 0;
  left: 0;
  right: 0;
  pointer-events: auto;
  -webkit-tap-highlight-color: transparent;
  opacity: 0;
  touch-action: manipulation;
}
@layer cdk-overlay {
  .cdk-overlay-backdrop {
    z-index: 1000;
    transition: opacity 400ms cubic-bezier(0.25, 0.8, 0.25, 1);
  }
}
@media (prefers-reduced-motion) {
  .cdk-overlay-backdrop {
    transition-duration: 1ms;
  }
}

.cdk-overlay-backdrop-showing {
  opacity: 1;
}
@media (forced-colors: active) {
  .cdk-overlay-backdrop-showing {
    opacity: 0.6;
  }
}

@layer cdk-overlay {
  .cdk-overlay-dark-backdrop {
    background: rgba(0, 0, 0, 0.32);
  }
}

.cdk-overlay-transparent-backdrop {
  transition: visibility 1ms linear, opacity 1ms linear;
  visibility: hidden;
  opacity: 1;
}
.cdk-overlay-transparent-backdrop.cdk-overlay-backdrop-showing, .cdk-high-contrast-active .cdk-overlay-transparent-backdrop {
  opacity: 0;
  visibility: visible;
}

.cdk-overlay-backdrop-noop-animation {
  transition: none;
}

.cdk-overlay-connected-position-bounding-box {
  position: absolute;
  display: flex;
  flex-direction: column;
  min-width: 1px;
  min-height: 1px;
}
@layer cdk-overlay {
  .cdk-overlay-connected-position-bounding-box {
    z-index: 1000;
  }
}

.cdk-global-scrollblock {
  position: fixed;
  width: 100%;
  overflow-y: scroll;
}

.cdk-overlay-popover {
  background: none;
  border: none;
  padding: 0;
  outline: 0;
  overflow: visible;
  position: fixed;
  pointer-events: none;
  white-space: normal;
  color: inherit;
  text-decoration: none;
  width: 100%;
  height: 100%;
  inset: auto;
  top: 0;
  left: 0;
}
.cdk-overlay-popover::backdrop {
  display: none;
}
.cdk-overlay-popover .cdk-overlay-backdrop {
  position: fixed;
  z-index: auto;
}
`],encapsulation:2})}return t})(),Ku=(()=>{class t{_platform=d(be);_containerElement;_document=d(L);_styleLoader=d(ze);ngOnDestroy(){this._containerElement?.remove()}getContainerElement(){return this._loadStyles(),this._containerElement||this._createContainer(),this._containerElement}_createContainer(){let e="cdk-overlay-container";if(this._platform.isBrowser||Kg()){let r=this._document.querySelectorAll(`.${e}[platform="server"], .${e}[platform="test"]`);for(let o=0;o<r.length;o++)r[o].remove()}let i=this._document.createElement("div");i.classList.add(e),Kg()?i.setAttribute("platform","test"):this._platform.isBrowser||i.setAttribute("platform","server"),this._document.body.appendChild(i),this._containerElement=i}_loadStyles(){this._styleLoader.load(PD)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})(),nv=class{_renderer;_ngZone;element;_cleanupClick;_cleanupTransitionEnd;_fallbackTimeout;constructor(n,e,i,r){this._renderer=e,this._ngZone=i,this.element=n.createElement("div"),this.element.classList.add("cdk-overlay-backdrop"),this._cleanupClick=e.listen(this.element,"click",r)}detach(){this._ngZone.runOutsideAngular(()=>{let n=this.element;clearTimeout(this._fallbackTimeout),this._cleanupTransitionEnd?.(),this._cleanupTransitionEnd=this._renderer.listen(n,"transitionend",this.dispose),this._fallbackTimeout=setTimeout(this.dispose,500),n.style.pointerEvents="none",n.classList.remove("cdk-overlay-backdrop-showing")})}dispose=()=>{clearTimeout(this._fallbackTimeout),this._cleanupClick?.(),this._cleanupTransitionEnd?.(),this._cleanupClick=this._cleanupTransitionEnd=this._fallbackTimeout=void 0,this.element.remove()}};function iv(t){return t&&t.nodeType===1}var ev=new Set;var bs=class{_portalOutlet;_host;_pane;_config;_ngZone;_keyboardDispatcher;_document;_location;_outsideClickDispatcher;_animationsDisabled;_injector;_renderer;_backdropClick=new E;_attachments=new E;_detachments=new E;_positionStrategy;_scrollStrategy;_locationChanges=pe.EMPTY;_backdropRef=null;_detachContentMutationObserver;_detachContentAfterRenderRef;_disposed=!1;_previousHostParent;_keydownEvents=new E;_outsidePointerEvents=new E;_afterNextRenderRef;constructor(n,e,i,r,o,s,a,l,c,u=!1,f,h){this._portalOutlet=n,this._host=e,this._pane=i,this._config=r,this._ngZone=o,this._keyboardDispatcher=s,this._document=a,this._location=l,this._outsideClickDispatcher=c,this._animationsDisabled=u,this._injector=f,this._renderer=h,r.scrollStrategy&&(this._scrollStrategy=r.scrollStrategy,this._scrollStrategy.attach(this)),this._positionStrategy=r.positionStrategy}get overlayElement(){return this._pane}get backdropElement(){return this._backdropRef?.element||null}get hostElement(){return this._host}get eventPredicate(){return this._config?.eventPredicate||null}attach(n){if(this._disposed)return null;this._attachHost();let e=this._portalOutlet.attach(n);return this._positionStrategy?.attach(this),this._updateStackingOrder(),this._updateElementSize(),this._updateElementDirection(),ev.add(this),this._scrollStrategy&&this._scrollStrategy.enable(),this._afterNextRenderRef?.destroy(),this._afterNextRenderRef=Qe(()=>{this.hasAttached()&&this.updatePosition()},{injector:this._injector}),this._togglePointerEvents(!0),this._config.hasBackdrop&&this._attachBackdrop(),this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!0),this._attachments.next(),this._completeDetachContent(),this._keyboardDispatcher.add(this),this._config.disposeOnNavigation&&(this._locationChanges=this._location.subscribe(()=>this.dispose())),this._outsideClickDispatcher.add(this),typeof e?.onDestroy=="function"&&e.onDestroy(()=>{this.hasAttached()&&this._ngZone.runOutsideAngular(()=>Promise.resolve().then(()=>this.detach()))}),e}detach(){if(!this.hasAttached())return;this.detachBackdrop(),this._togglePointerEvents(!1),this._positionStrategy&&this._positionStrategy.detach&&this._positionStrategy.detach(),this._scrollStrategy&&this._scrollStrategy.disable();let n=this._portalOutlet.detach();return this._detachments.next(),this._completeDetachContent(),this._keyboardDispatcher.remove(this),this._detachContentWhenEmpty(),this._locationChanges.unsubscribe(),this._outsideClickDispatcher.remove(this),ev.delete(this),n}dispose(){if(this._disposed)return;let n=this.hasAttached();this._positionStrategy&&this._positionStrategy.dispose(),this._disposeScrollStrategy(),this._backdropRef?.dispose(),this._locationChanges.unsubscribe(),this._keyboardDispatcher.remove(this),this._portalOutlet.dispose(),this._attachments.complete(),this._backdropClick.complete(),this._keydownEvents.complete(),this._outsidePointerEvents.complete(),this._outsideClickDispatcher.remove(this),this._host?.remove(),this._afterNextRenderRef?.destroy(),this._previousHostParent=this._pane=this._host=this._backdropRef=null,n&&this._detachments.next(),this._detachments.complete(),this._completeDetachContent(),this._disposed=!0,ev.delete(this)}hasAttached(){return this._portalOutlet.hasAttached()}backdropClick(){return this._backdropClick}attachments(){return this._attachments}detachments(){return this._detachments}keydownEvents(){return this._keydownEvents}outsidePointerEvents(){return this._outsidePointerEvents}getConfig(){return this._config}updatePosition(){this._positionStrategy&&this._positionStrategy.apply()}updatePositionStrategy(n){n!==this._positionStrategy&&(this._positionStrategy&&this._positionStrategy.dispose(),this._positionStrategy=n,this.hasAttached()&&(n.attach(this),this.updatePosition()))}updateSize(n){this._config=v(v({},this._config),n),this._updateElementSize()}setDirection(n){this._config=k(v({},this._config),{direction:n}),this._updateElementDirection()}addPanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!0)}removePanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!1)}getDirection(){let n=this._config.direction;return n?typeof n=="string"?n:n.value:"ltr"}updateScrollStrategy(n){n!==this._scrollStrategy&&(this._disposeScrollStrategy(),this._scrollStrategy=n,this.hasAttached()&&(n.attach(this),n.enable()))}_updateElementDirection(){this._host.setAttribute("dir",this.getDirection())}_updateElementSize(){if(!this._pane)return;let n=this._pane.style;n.width=Ze(this._config.width),n.height=Ze(this._config.height),n.minWidth=Ze(this._config.minWidth),n.minHeight=Ze(this._config.minHeight),n.maxWidth=Ze(this._config.maxWidth),n.maxHeight=Ze(this._config.maxHeight)}_togglePointerEvents(n){this._pane.style.pointerEvents=n?"":"none"}_attachHost(){if(!this._host.parentElement){let n=this._config.usePopover?this._positionStrategy?.getPopoverInsertionPoint?.():null;iv(n)?n.after(this._host):n?.type==="parent"?n.element.appendChild(this._host):this._previousHostParent?.appendChild(this._host)}if(this._config.usePopover)try{this._host.showPopover()}catch{}}_attachBackdrop(){let n="cdk-overlay-backdrop-showing";this._backdropRef?.dispose(),this._backdropRef=new nv(this._document,this._renderer,this._ngZone,e=>{this._backdropClick.next(e)}),this._animationsDisabled&&this._backdropRef.element.classList.add("cdk-overlay-backdrop-noop-animation"),this._config.backdropClass&&this._toggleClasses(this._backdropRef.element,this._config.backdropClass,!0),this._config.usePopover?this._host.prepend(this._backdropRef.element):this._host.parentElement.insertBefore(this._backdropRef.element,this._host),!this._animationsDisabled&&typeof requestAnimationFrame<"u"?this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>this._backdropRef?.element.classList.add(n))}):this._backdropRef.element.classList.add(n)}_updateStackingOrder(){!this._config.usePopover&&this._host.nextSibling&&this._host.parentNode.appendChild(this._host)}detachBackdrop(){this._animationsDisabled?(this._backdropRef?.dispose(),this._backdropRef=null):this._backdropRef?.detach()}_toggleClasses(n,e,i){let r=ps(e||[]).filter(o=>!!o);r.length&&(i?n.classList.add(...r):n.classList.remove(...r))}_detachContentWhenEmpty(){let n=!1;try{this._detachContentAfterRenderRef=Qe(()=>{n=!0,this._detachContent()},{injector:this._injector})}catch(e){if(n)throw e;this._detachContent()}globalThis.MutationObserver&&this._pane&&(this._detachContentMutationObserver||=new globalThis.MutationObserver(()=>{this._detachContent()}),this._detachContentMutationObserver.observe(this._pane,{childList:!0}))}_detachContent(){(!this._pane||!this._host||this._pane.children.length===0)&&(this._pane&&this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!1),this._host&&this._host.parentElement&&(this._previousHostParent=this._host.parentElement,this._host.remove()),this._completeDetachContent())}_completeDetachContent(){this._detachContentAfterRenderRef?.destroy(),this._detachContentAfterRenderRef=void 0,this._detachContentMutationObserver?.disconnect()}_disposeScrollStrategy(){let n=this._scrollStrategy;n?.disable(),n?.detach?.()}},xD="cdk-overlay-connected-position-bounding-box",rP=/([A-Za-z%]+)$/;function fo(t,n){return new Zu(n,t.get(yi),t.get(L),t.get(be),t.get(Ku))}var Zu=class{_viewportRuler;_document;_platform;_overlayContainer;_overlayRef;_isInitialRender=!1;_lastBoundingBoxSize={width:0,height:0};_isPushed=!1;_canPush=!0;_growAfterOpen=!1;_hasFlexibleDimensions=!0;_positionLocked=!1;_originRect;_overlayRect;_viewportRect;_containerRect;_viewportMargin=0;_scrollables=[];_preferredPositions=[];_origin;_pane;_isDisposed=!1;_boundingBox=null;_lastPosition=null;_lastScrollVisibility=null;_positionChanges=new E;_resizeSubscription=pe.EMPTY;_offsetX=0;_offsetY=0;_transformOriginSelector;_appliedPanelClasses=[];_previousPushAmount=null;_popoverLocation="global";positionChanges=this._positionChanges;get positions(){return this._preferredPositions}constructor(n,e,i,r,o){this._viewportRuler=e,this._document=i,this._platform=r,this._overlayContainer=o,this.setOrigin(n)}attach(n){this._overlayRef&&this._overlayRef,this._validatePositions(),n.hostElement.classList.add(xD),this._overlayRef=n,this._boundingBox=n.hostElement,this._pane=n.overlayElement,this._isDisposed=!1,this._isInitialRender=!0,this._lastPosition=null,this._resizeSubscription.unsubscribe(),this._resizeSubscription=this._viewportRuler.change().subscribe(()=>{this._isInitialRender=!0,this.apply()})}apply(){if(this._isDisposed||!this._platform.isBrowser)return;if(!this._isInitialRender&&this._positionLocked&&this._lastPosition){this.reapplyLastPosition();return}this._clearPanelClasses(),this._resetOverlayElementStyles(),this._resetBoundingBoxStyles(),this._viewportRect=this._getNarrowedViewportRect(),this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._containerRect=this._getContainerRect();let n=this._originRect,e=this._overlayRect,i=this._viewportRect,r=this._containerRect,o=[],s;for(let a of this._preferredPositions){let l=this._getOriginPoint(n,r,a),c=this._getOverlayPoint(l,e,a),u=this._getOverlayFit(c,e,i,a);if(u.isCompletelyWithinViewport){this._isPushed=!1,this._applyPosition(a,l);return}if(this._canFitWithFlexibleDimensions(u,c,i)){o.push({position:a,origin:l,overlayRect:e,boundingBoxRect:this._calculateBoundingBoxRect(l,a)});continue}(!s||s.overlayFit.visibleArea<u.visibleArea)&&(s={overlayFit:u,overlayPoint:c,originPoint:l,position:a,overlayRect:e})}if(o.length){let a=null,l=-1;for(let c of o){let u=c.boundingBoxRect.width*c.boundingBoxRect.height*(c.position.weight||1);u>l&&(l=u,a=c)}this._isPushed=!1,this._applyPosition(a.position,a.origin);return}if(this._canPush){this._isPushed=!0,this._applyPosition(s.position,s.originPoint);return}this._applyPosition(s.position,s.originPoint)}detach(){this._clearPanelClasses(),this._lastPosition=null,this._previousPushAmount=null,this._resizeSubscription.unsubscribe()}dispose(){this._isDisposed||(this._boundingBox&&uo(this._boundingBox.style,{top:"",left:"",right:"",bottom:"",height:"",width:"",alignItems:"",justifyContent:""}),this._pane&&this._resetOverlayElementStyles(),this._overlayRef&&this._overlayRef.hostElement.classList.remove(xD),this.detach(),this._positionChanges.complete(),this._overlayRef=this._boundingBox=null,this._isDisposed=!0)}reapplyLastPosition(){if(this._isDisposed||!this._platform.isBrowser)return;let n=this._lastPosition;n?(this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._viewportRect=this._getNarrowedViewportRect(),this._containerRect=this._getContainerRect(),this._applyPosition(n,this._getOriginPoint(this._originRect,this._containerRect,n))):this.apply()}withScrollableContainers(n){return this._scrollables=n,this}withPositions(n){return this._preferredPositions=n,n.indexOf(this._lastPosition)===-1&&(this._lastPosition=null),this._validatePositions(),this}withViewportMargin(n){return this._viewportMargin=n,this}withFlexibleDimensions(n=!0){return this._hasFlexibleDimensions=n,this}withGrowAfterOpen(n=!0){return this._growAfterOpen=n,this}withPush(n=!0){return this._canPush=n,this}withLockedPosition(n=!0){return this._positionLocked=n,this}setOrigin(n){return this._origin=n,this}withDefaultOffsetX(n){return this._offsetX=n,this}withDefaultOffsetY(n){return this._offsetY=n,this}withTransformOriginOn(n){return this._transformOriginSelector=n,this}withPopoverLocation(n){return this._popoverLocation=n,this}getPopoverInsertionPoint(){return this._popoverLocation==="global"?null:this._popoverLocation!=="inline"?this._popoverLocation:this._origin instanceof O?this._origin.nativeElement:iv(this._origin)?this._origin:null}_getOriginPoint(n,e,i){let r;if(i.originX=="center")r=n.left+n.width/2;else{let s=this._isRtl()?n.right:n.left,a=this._isRtl()?n.left:n.right;r=i.originX=="start"?s:a}e.left<0&&(r-=e.left);let o;return i.originY=="center"?o=n.top+n.height/2:o=i.originY=="top"?n.top:n.bottom,e.top<0&&(o-=e.top),{x:r,y:o}}_getOverlayPoint(n,e,i){let r;i.overlayX=="center"?r=-e.width/2:i.overlayX==="start"?r=this._isRtl()?-e.width:0:r=this._isRtl()?0:-e.width;let o;return i.overlayY=="center"?o=-e.height/2:o=i.overlayY=="top"?0:-e.height,{x:n.x+r,y:n.y+o}}_getOverlayFit(n,e,i,r){let o=MD(e),{x:s,y:a}=n,l=this._getOffset(r,"x"),c=this._getOffset(r,"y");l&&(s+=l),c&&(a+=c);let u=0-s,f=s+o.width-i.width,h=0-a,p=a+o.height-i.height,S=this._subtractOverflows(o.width,u,f),C=this._subtractOverflows(o.height,h,p),$=S*C;return{visibleArea:$,isCompletelyWithinViewport:o.width*o.height===$,fitsInViewportVertically:C===o.height,fitsInViewportHorizontally:S==o.width}}_canFitWithFlexibleDimensions(n,e,i){if(this._hasFlexibleDimensions){let r=i.bottom-e.y,o=i.right-e.x,s=ID(this._overlayRef.getConfig().minHeight),a=ID(this._overlayRef.getConfig().minWidth),l=n.fitsInViewportVertically||s!=null&&s<=r,c=n.fitsInViewportHorizontally||a!=null&&a<=o;return l&&c}return!1}_pushOverlayOnScreen(n,e,i){if(this._previousPushAmount&&this._positionLocked)return{x:n.x+this._previousPushAmount.x,y:n.y+this._previousPushAmount.y};let r=MD(e),o=this._viewportRect,s=Math.max(n.x+r.width-o.width,0),a=Math.max(n.y+r.height-o.height,0),l=Math.max(o.top-i.top-n.y,0),c=Math.max(o.left-i.left-n.x,0),u=0,f=0;return r.width<=o.width?u=c||-s:u=n.x<this._getViewportMarginStart()?o.left-i.left-n.x:0,r.height<=o.height?f=l||-a:f=n.y<this._getViewportMarginTop()?o.top-i.top-n.y:0,this._previousPushAmount={x:u,y:f},{x:n.x+u,y:n.y+f}}_applyPosition(n,e){if(this._setTransformOrigin(n),this._setOverlayElementStyles(e,n),this._setBoundingBoxStyles(e,n),n.panelClass&&this._addPanelClasses(n.panelClass),this._positionChanges.observers.length){let i=this._getScrollVisibility();if(n!==this._lastPosition||!this._lastScrollVisibility||!oP(this._lastScrollVisibility,i)){let r=new Yu(n,i);this._positionChanges.next(r)}this._lastScrollVisibility=i}this._lastPosition=n,this._isInitialRender=!1}_setTransformOrigin(n){if(!this._transformOriginSelector)return;let e=this._boundingBox.querySelectorAll(this._transformOriginSelector),i,r=n.overlayY;n.overlayX==="center"?i="center":this._isRtl()?i=n.overlayX==="start"?"right":"left":i=n.overlayX==="start"?"left":"right";for(let o=0;o<e.length;o++)e[o].style.transformOrigin=`${i} ${r}`}_calculateBoundingBoxRect(n,e){let i=this._viewportRect,r=this._isRtl(),o,s,a;if(e.overlayY==="top")s=n.y,o=i.height-s+this._getViewportMarginBottom();else if(e.overlayY==="bottom")a=i.height-n.y+this._getViewportMarginTop()+this._getViewportMarginBottom(),o=i.height-a+this._getViewportMarginTop();else{let p=Math.min(i.bottom-n.y+i.top,n.y),S=this._lastBoundingBoxSize.height;o=p*2,s=n.y-p,o>S&&!this._isInitialRender&&!this._growAfterOpen&&(s=n.y-S/2)}let l=e.overlayX==="start"&&!r||e.overlayX==="end"&&r,c=e.overlayX==="end"&&!r||e.overlayX==="start"&&r,u,f,h;if(c)h=i.width-n.x+this._getViewportMarginStart()+this._getViewportMarginEnd(),u=n.x-this._getViewportMarginStart();else if(l)f=n.x,u=i.right-n.x-this._getViewportMarginEnd();else{let p=Math.min(i.right-n.x+i.left,n.x),S=this._lastBoundingBoxSize.width;u=p*2,f=n.x-p,u>S&&!this._isInitialRender&&!this._growAfterOpen&&(f=n.x-S/2)}return{top:s,left:f,bottom:a,right:h,width:u,height:o}}_setBoundingBoxStyles(n,e){let i=this._calculateBoundingBoxRect(n,e);!this._isInitialRender&&!this._growAfterOpen&&(i.height=Math.min(i.height,this._lastBoundingBoxSize.height),i.width=Math.min(i.width,this._lastBoundingBoxSize.width));let r={};if(this._hasExactPosition())r.top=r.left="0",r.bottom=r.right="auto",r.maxHeight=r.maxWidth="",r.width=r.height="100%";else{let o=this._overlayRef.getConfig().maxHeight,s=this._overlayRef.getConfig().maxWidth;r.width=Ze(i.width),r.height=Ze(i.height),r.top=Ze(i.top)||"auto",r.bottom=Ze(i.bottom)||"auto",r.left=Ze(i.left)||"auto",r.right=Ze(i.right)||"auto",e.overlayX==="center"?r.alignItems="center":r.alignItems=e.overlayX==="end"?"flex-end":"flex-start",e.overlayY==="center"?r.justifyContent="center":r.justifyContent=e.overlayY==="bottom"?"flex-end":"flex-start",o&&(r.maxHeight=Ze(o)),s&&(r.maxWidth=Ze(s))}this._lastBoundingBoxSize=i,uo(this._boundingBox.style,r)}_resetBoundingBoxStyles(){uo(this._boundingBox.style,{top:"0",left:"0",right:"0",bottom:"0",height:"",width:"",alignItems:"",justifyContent:""})}_resetOverlayElementStyles(){uo(this._pane.style,{top:"",left:"",bottom:"",right:"",position:"",transform:""})}_setOverlayElementStyles(n,e){let i={},r=this._hasExactPosition(),o=this._hasFlexibleDimensions,s=this._overlayRef.getConfig();if(r){let u=this._viewportRuler.getViewportScrollPosition();uo(i,this._getExactOverlayY(e,n,u)),uo(i,this._getExactOverlayX(e,n,u))}else i.position="static";let a="",l=this._getOffset(e,"x"),c=this._getOffset(e,"y");l&&(a+=`translateX(${l}px) `),c&&(a+=`translateY(${c}px)`),i.transform=a.trim(),s.maxHeight&&(r?i.maxHeight=Ze(s.maxHeight):o&&(i.maxHeight="")),s.maxWidth&&(r?i.maxWidth=Ze(s.maxWidth):o&&(i.maxWidth="")),uo(this._pane.style,i)}_getExactOverlayY(n,e,i){let r={top:"",bottom:""},o=this._getOverlayPoint(e,this._overlayRect,n);if(this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i)),n.overlayY==="bottom"){let s=this._document.documentElement.clientHeight;r.bottom=`${s-(o.y+this._overlayRect.height)}px`}else r.top=Ze(o.y);return r}_getExactOverlayX(n,e,i){let r={left:"",right:""},o=this._getOverlayPoint(e,this._overlayRect,n);this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i));let s;if(this._isRtl()?s=n.overlayX==="end"?"left":"right":s=n.overlayX==="end"?"right":"left",s==="right"){let a=this._document.documentElement.clientWidth;r.right=`${a-(o.x+this._overlayRect.width)}px`}else r.left=Ze(o.x);return r}_getScrollVisibility(){let n=this._getOriginRect(),e=this._pane.getBoundingClientRect(),i=this._scrollables.map(r=>r.getElementRef().nativeElement.getBoundingClientRect());return{isOriginClipped:ED(n,i),isOriginOutsideView:tv(n,i),isOverlayClipped:ED(e,i),isOverlayOutsideView:tv(e,i)}}_subtractOverflows(n,...e){return e.reduce((i,r)=>i-Math.max(r,0),n)}_getNarrowedViewportRect(){let n=this._document.documentElement.clientWidth,e=this._document.documentElement.clientHeight,i=this._viewportRuler.getViewportScrollPosition();return{top:i.top+this._getViewportMarginTop(),left:i.left+this._getViewportMarginStart(),right:i.left+n-this._getViewportMarginEnd(),bottom:i.top+e-this._getViewportMarginBottom(),width:n-this._getViewportMarginStart()-this._getViewportMarginEnd(),height:e-this._getViewportMarginTop()-this._getViewportMarginBottom()}}_isRtl(){return this._overlayRef.getDirection()==="rtl"}_hasExactPosition(){return!this._hasFlexibleDimensions||this._isPushed}_getOffset(n,e){return e==="x"?n.offsetX==null?this._offsetX:n.offsetX:n.offsetY==null?this._offsetY:n.offsetY}_validatePositions(){}_addPanelClasses(n){this._pane&&ps(n).forEach(e=>{e!==""&&this._appliedPanelClasses.indexOf(e)===-1&&(this._appliedPanelClasses.push(e),this._pane.classList.add(e))})}_clearPanelClasses(){this._pane&&(this._appliedPanelClasses.forEach(n=>{this._pane.classList.remove(n)}),this._appliedPanelClasses=[])}_getViewportMarginStart(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.start??0}_getViewportMarginEnd(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.end??0}_getViewportMarginTop(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.top??0}_getViewportMarginBottom(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.bottom??0}_getOriginRect(){let n=this._origin;if(n instanceof O)return n.nativeElement.getBoundingClientRect();if(n instanceof Element)return n.getBoundingClientRect();let e=n.width||0,i=n.height||0;return{top:n.y,bottom:n.y+i,left:n.x,right:n.x+e,height:i,width:e}}_getContainerRect(){let n=this._overlayRef.getConfig().usePopover&&this._popoverLocation!=="global",e=this._overlayContainer.getContainerElement();n&&(e.style.display="block");let i=e.getBoundingClientRect();return n&&(e.style.display=""),i}};function uo(t,n){for(let e in n)n.hasOwnProperty(e)&&(t[e]=n[e]);return t}function ID(t){if(typeof t!="number"&&t!=null){let[n,e]=t.split(rP);return!e||e==="px"?parseFloat(n):null}return t||null}function MD(t){return{top:Math.floor(t.top),right:Math.floor(t.right),bottom:Math.floor(t.bottom),left:Math.floor(t.left),width:Math.floor(t.width),height:Math.floor(t.height)}}function oP(t,n){return t===n?!0:t.isOriginClipped===n.isOriginClipped&&t.isOriginOutsideView===n.isOriginOutsideView&&t.isOverlayClipped===n.isOverlayClipped&&t.isOverlayOutsideView===n.isOverlayOutsideView}var TD="cdk-global-overlay-wrapper";function Wn(t){return new Xu}var Xu=class{_overlayRef;_cssPosition="static";_topOffset="";_bottomOffset="";_alignItems="";_xPosition="";_xOffset="";_width="";_height="";_isDisposed=!1;attach(n){let e=n.getConfig();this._overlayRef=n,this._width&&!e.width&&n.updateSize({width:this._width}),this._height&&!e.height&&n.updateSize({height:this._height}),n.hostElement.classList.add(TD),this._isDisposed=!1}top(n=""){return this._bottomOffset="",this._topOffset=n,this._alignItems="flex-start",this}left(n=""){return this._xOffset=n,this._xPosition="left",this}bottom(n=""){return this._topOffset="",this._bottomOffset=n,this._alignItems="flex-end",this}right(n=""){return this._xOffset=n,this._xPosition="right",this}start(n=""){return this._xOffset=n,this._xPosition="start",this}end(n=""){return this._xOffset=n,this._xPosition="end",this}width(n=""){return this._overlayRef?this._overlayRef.updateSize({width:n}):this._width=n,this}height(n=""){return this._overlayRef?this._overlayRef.updateSize({height:n}):this._height=n,this}centerHorizontally(n=""){return this.left(n),this._xPosition="center",this}centerVertically(n=""){return this.top(n),this._alignItems="center",this}apply(){if(!this._overlayRef||!this._overlayRef.hasAttached())return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement.style,i=this._overlayRef.getConfig(),{width:r,height:o,maxWidth:s,maxHeight:a}=i,l=(r==="100%"||r==="100vw")&&(!s||s==="100%"||s==="100vw"),c=(o==="100%"||o==="100vh")&&(!a||a==="100%"||a==="100vh"),u=this._xPosition,f=this._xOffset,h=this._overlayRef.getConfig().direction==="rtl",p="",S="",C="";l?C="flex-start":u==="center"?(C="center",h?S=f:p=f):h?u==="left"||u==="end"?(C="flex-end",p=f):(u==="right"||u==="start")&&(C="flex-start",S=f):u==="left"||u==="start"?(C="flex-start",p=f):(u==="right"||u==="end")&&(C="flex-end",S=f),n.position=this._cssPosition,n.marginLeft=l?"0":p,n.marginTop=c?"0":this._topOffset,n.marginBottom=this._bottomOffset,n.marginRight=l?"0":S,e.justifyContent=C,e.alignItems=c?"flex-start":this._alignItems}dispose(){if(this._isDisposed||!this._overlayRef)return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement,i=e.style;e.classList.remove(TD),i.justifyContent=i.alignItems=n.marginTop=n.marginBottom=n.marginLeft=n.marginRight=n.position="",this._overlayRef=null,this._isDisposed=!0}},FD=(()=>{class t{_injector=d(R);global(){return Wn()}flexibleConnectedTo(e){return fo(this._injector,e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})(),yl=new b("OVERLAY_DEFAULT_CONFIG");function In(t,n){t.get(ze).load(PD);let e=t.get(Ku),i=t.get(L),r=t.get(xe),o=t.get(Ft),s=t.get(ct),a=t.get(Re,null,{optional:!0})||t.get(Ke).createRenderer(null,null),l=new wn(n),c=t.get(yl,null,{optional:!0})?.usePopover??!0;l.direction=l.direction||s.value,!i.body||!("showPopover"in i.body)?l.usePopover=!1:l.usePopover=n?.usePopover??c;let u=i.createElement("div"),f=i.createElement("div");u.id=r.getId("cdk-overlay-"),u.classList.add("cdk-overlay-pane"),f.appendChild(u),l.usePopover&&(f.setAttribute("popover","manual"),f.classList.add("cdk-overlay-popover"));let h=l.usePopover?l.positionStrategy?.getPopoverInsertionPoint?.():null;return iv(h)?h.after(f):h?.type==="parent"?h.element.appendChild(f):e.getContainerElement().appendChild(f),new bs(new hl(u,o,t),f,u,l,t.get(N),t.get(RD),i,t.get(qi),t.get(OD),n?.disableAnimations??t.get(pa,null,{optional:!0})==="NoopAnimations",t.get(Ie),a)}var LD=(()=>{class t{scrollStrategies=d(kD);_positionBuilder=d(FD);_injector=d(R);create(e){return In(this._injector,e)}position(){return this._positionBuilder}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})(),sP=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"}],aP=new b("cdk-connected-overlay-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(R);return()=>Si(t)}}),_s=(()=>{class t{elementRef=d(O);static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,selectors:[["","cdk-overlay-origin",""],["","overlay-origin",""],["","cdkOverlayOrigin",""]],exportAs:["cdkOverlayOrigin"]})}return t})(),VD=new b("cdk-connected-overlay-default-config"),Qu=(()=>{class t{_dir=d(ct,{optional:!0});_injector=d(R);_overlayRef;_templatePortal;_backdropSubscription=pe.EMPTY;_attachSubscription=pe.EMPTY;_detachSubscription=pe.EMPTY;_positionSubscription=pe.EMPTY;_offsetX;_offsetY;_position;_scrollStrategyFactory=d(aP);_ngZone=d(N);origin;positions;positionStrategy;get offsetX(){return this._offsetX}set offsetX(e){this._offsetX=e,this._position&&this._updatePositionStrategy(this._position)}get offsetY(){return this._offsetY}set offsetY(e){this._offsetY=e,this._position&&this._updatePositionStrategy(this._position)}width;height;minWidth;minHeight;backdropClass;panelClass;viewportMargin=0;scrollStrategy;open=!1;disableClose=!1;transformOriginSelector;hasBackdrop=!1;lockPosition=!1;flexibleDimensions=!1;growAfterOpen=!1;push=!1;disposeOnNavigation=!1;usePopover;matchWidth=!1;set _config(e){typeof e!="string"&&this._assignConfig(e)}backdropClick=new W;positionChange=new W;attach=new W;detach=new W;overlayKeydown=new W;overlayOutsideClick=new W;constructor(){let e=d(Ct),i=d(bt),r=d(VD,{optional:!0}),o=d(yl,{optional:!0});this.usePopover=o?.usePopover===!1?null:"global",this._templatePortal=new Dn(e,i),this.scrollStrategy=this._scrollStrategyFactory(),r&&this._assignConfig(r)}get overlayRef(){return this._overlayRef}get dir(){return this._dir?this._dir.value:"ltr"}ngOnDestroy(){this._attachSubscription.unsubscribe(),this._detachSubscription.unsubscribe(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this._overlayRef?.dispose()}ngOnChanges(e){this._position&&(this._updatePositionStrategy(this._position),this._overlayRef?.updateSize({width:this._getWidth(),minWidth:this.minWidth,height:this.height,minHeight:this.minHeight}),e.origin&&this.open&&this._position.apply()),e.open&&(this.open?this.attachOverlay():this.detachOverlay())}_createOverlay(){(!this.positions||!this.positions.length)&&(this.positions=sP);let e=this._overlayRef=In(this._injector,this._buildConfig());this._attachSubscription=e.attachments().subscribe(()=>this.attach.emit()),this._detachSubscription=e.detachments().subscribe(()=>this.detach.emit()),e.keydownEvents().subscribe(i=>{this.overlayKeydown.next(i),i.keyCode===27&&!this.disableClose&&!$e(i)&&(i.preventDefault(),this.detachOverlay())}),this._overlayRef.outsidePointerEvents().subscribe(i=>{let r=this._getOriginElement(),o=yt(i);(!r||r!==o&&!r.contains(o))&&this.overlayOutsideClick.next(i)})}_buildConfig(){let e=this._position=this.positionStrategy||this._createPositionStrategy(),i=new wn({direction:this._dir||"ltr",positionStrategy:e,scrollStrategy:this.scrollStrategy,hasBackdrop:this.hasBackdrop,disposeOnNavigation:this.disposeOnNavigation,usePopover:!!this.usePopover});return(this.height||this.height===0)&&(i.height=this.height),(this.minWidth||this.minWidth===0)&&(i.minWidth=this.minWidth),(this.minHeight||this.minHeight===0)&&(i.minHeight=this.minHeight),this.backdropClass&&(i.backdropClass=this.backdropClass),this.panelClass&&(i.panelClass=this.panelClass),i}_updatePositionStrategy(e){let i=this.positions.map(r=>({originX:r.originX,originY:r.originY,overlayX:r.overlayX,overlayY:r.overlayY,offsetX:r.offsetX||this.offsetX,offsetY:r.offsetY||this.offsetY,panelClass:r.panelClass||void 0}));return e.setOrigin(this._getOrigin()).withPositions(i).withFlexibleDimensions(this.flexibleDimensions).withPush(this.push).withGrowAfterOpen(this.growAfterOpen).withViewportMargin(this.viewportMargin).withLockedPosition(this.lockPosition).withTransformOriginOn(this.transformOriginSelector).withPopoverLocation(this.usePopover===null?"global":this.usePopover)}_createPositionStrategy(){let e=fo(this._injector,this._getOrigin());return this._updatePositionStrategy(e),e}_getOrigin(){return this.origin instanceof _s?this.origin.elementRef:this.origin}_getOriginElement(){return this.origin instanceof _s?this.origin.elementRef.nativeElement:this.origin instanceof O?this.origin.nativeElement:typeof Element<"u"&&this.origin instanceof Element?this.origin:null}_getWidth(){return this.width?this.width:this.matchWidth?this._getOriginElement()?.getBoundingClientRect?.().width:void 0}attachOverlay(){this._overlayRef||this._createOverlay();let e=this._overlayRef;e.getConfig().hasBackdrop=this.hasBackdrop,e.updateSize({width:this._getWidth()}),e.hasAttached()||e.attach(this._templatePortal),this.hasBackdrop?this._backdropSubscription=e.backdropClick().subscribe(i=>this.backdropClick.emit(i)):this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.positionChange.observers.length>0&&(this._positionSubscription=this._position.positionChanges.pipe(ah(()=>this.positionChange.observers.length>0)).subscribe(i=>{this._ngZone.run(()=>this.positionChange.emit(i)),this.positionChange.observers.length===0&&this._positionSubscription.unsubscribe()})),this.open=!0}detachOverlay(){this._overlayRef?.detach(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.open=!1}_assignConfig(e){this.origin=e.origin??this.origin,this.positions=e.positions??this.positions,this.positionStrategy=e.positionStrategy??this.positionStrategy,this.offsetX=e.offsetX??this.offsetX,this.offsetY=e.offsetY??this.offsetY,this.width=e.width??this.width,this.height=e.height??this.height,this.minWidth=e.minWidth??this.minWidth,this.minHeight=e.minHeight??this.minHeight,this.backdropClass=e.backdropClass??this.backdropClass,this.panelClass=e.panelClass??this.panelClass,this.viewportMargin=e.viewportMargin??this.viewportMargin,this.scrollStrategy=e.scrollStrategy??this.scrollStrategy,this.disableClose=e.disableClose??this.disableClose,this.transformOriginSelector=e.transformOriginSelector??this.transformOriginSelector,this.hasBackdrop=e.hasBackdrop??this.hasBackdrop,this.lockPosition=e.lockPosition??this.lockPosition,this.flexibleDimensions=e.flexibleDimensions??this.flexibleDimensions,this.growAfterOpen=e.growAfterOpen??this.growAfterOpen,this.push=e.push??this.push,this.disposeOnNavigation=e.disposeOnNavigation??this.disposeOnNavigation,this.usePopover=e.usePopover??this.usePopover,this.matchWidth=e.matchWidth??this.matchWidth}static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,selectors:[["","cdk-connected-overlay",""],["","connected-overlay",""],["","cdkConnectedOverlay",""]],inputs:{origin:[0,"cdkConnectedOverlayOrigin","origin"],positions:[0,"cdkConnectedOverlayPositions","positions"],positionStrategy:[0,"cdkConnectedOverlayPositionStrategy","positionStrategy"],offsetX:[0,"cdkConnectedOverlayOffsetX","offsetX"],offsetY:[0,"cdkConnectedOverlayOffsetY","offsetY"],width:[0,"cdkConnectedOverlayWidth","width"],height:[0,"cdkConnectedOverlayHeight","height"],minWidth:[0,"cdkConnectedOverlayMinWidth","minWidth"],minHeight:[0,"cdkConnectedOverlayMinHeight","minHeight"],backdropClass:[0,"cdkConnectedOverlayBackdropClass","backdropClass"],panelClass:[0,"cdkConnectedOverlayPanelClass","panelClass"],viewportMargin:[0,"cdkConnectedOverlayViewportMargin","viewportMargin"],scrollStrategy:[0,"cdkConnectedOverlayScrollStrategy","scrollStrategy"],open:[0,"cdkConnectedOverlayOpen","open"],disableClose:[0,"cdkConnectedOverlayDisableClose","disableClose"],transformOriginSelector:[0,"cdkConnectedOverlayTransformOriginOn","transformOriginSelector"],hasBackdrop:[2,"cdkConnectedOverlayHasBackdrop","hasBackdrop",P],lockPosition:[2,"cdkConnectedOverlayLockPosition","lockPosition",P],flexibleDimensions:[2,"cdkConnectedOverlayFlexibleDimensions","flexibleDimensions",P],growAfterOpen:[2,"cdkConnectedOverlayGrowAfterOpen","growAfterOpen",P],push:[2,"cdkConnectedOverlayPush","push",P],disposeOnNavigation:[2,"cdkConnectedOverlayDisposeOnNavigation","disposeOnNavigation",P],usePopover:[0,"cdkConnectedOverlayUsePopover","usePopover"],matchWidth:[2,"cdkConnectedOverlayMatchWidth","matchWidth",P],_config:[0,"cdkConnectedOverlay","_config"]},outputs:{backdropClick:"backdropClick",positionChange:"positionChange",attach:"attach",detach:"detach",overlayKeydown:"overlayKeydown",overlayOutsideClick:"overlayOutsideClick"},exportAs:["cdkConnectedOverlay"],features:[at]})}return t})(),on=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({providers:[LD],imports:[ve,vi,Jg,Jg]})}return t})();function lP(t,n){}var or=class{viewContainerRef;injector;id;role="dialog";panelClass="";hasBackdrop=!0;backdropClass="";disableClose=!1;closePredicate;width="";height="";minWidth;minHeight;maxWidth;maxHeight;positionStrategy;data=null;direction;ariaDescribedBy=null;ariaLabelledBy=null;ariaLabel=null;ariaModal=!1;autoFocus="first-tabbable";restoreFocus=!0;scrollStrategy;closeOnNavigation=!0;closeOnDestroy=!0;closeOnOverlayDetachments=!0;disableAnimations=!1;providers;container;templateContext;bindings};var Sl=(()=>{class t extends tr{_elementRef=d(O);_focusTrapFactory=d(Xg);_config;_interactivityChecker=d(Zg);_ngZone=d(N);_focusMonitor=d(rn);_renderer=d(Re);_changeDetectorRef=d(Oe);_injector=d(R);_platform=d(be);_document=d(L);_portalOutlet;_focusTrapped=new E;_focusTrap=null;_elementFocusedBeforeDialogWasOpened=null;_closeInteractionType=null;_ariaLabelledByQueue=[];_isDestroyed=!1;constructor(){super(),this._config=d(or,{optional:!0})||new or,this._config.ariaLabelledBy&&this._ariaLabelledByQueue.push(this._config.ariaLabelledBy)}_addAriaLabelledBy(e){this._ariaLabelledByQueue.push(e),this._changeDetectorRef.markForCheck()}_removeAriaLabelledBy(e){let i=this._ariaLabelledByQueue.indexOf(e);i>-1&&(this._ariaLabelledByQueue.splice(i,1),this._changeDetectorRef.markForCheck())}_contentAttached(){this._initializeFocusTrap(),this._captureInitialFocus()}_captureInitialFocus(){this._trapFocus()}ngOnDestroy(){this._focusTrapped.complete(),this._isDestroyed=!0,this._restoreFocus()}attachComponentPortal(e){this._portalOutlet.hasAttached();let i=this._portalOutlet.attachComponentPortal(e);return this._contentAttached(),i}attachTemplatePortal(e){this._portalOutlet.hasAttached();let i=this._portalOutlet.attachTemplatePortal(e);return this._contentAttached(),i}attachDomPortal=e=>{this._portalOutlet.hasAttached();let i=this._portalOutlet.attachDomPortal(e);return this._contentAttached(),i};_recaptureFocus(){this._containsFocus()||this._trapFocus()}_forceFocus(e,i){this._interactivityChecker.isFocusable(e)||(e.tabIndex=-1,this._ngZone.runOutsideAngular(()=>{let r=()=>{o(),s(),e.removeAttribute("tabindex")},o=this._renderer.listen(e,"blur",r),s=this._renderer.listen(e,"mousedown",r)})),e.focus(i)}_focusByCssSelector(e,i){let r=this._elementRef.nativeElement.querySelector(e);r&&this._forceFocus(r,i)}_trapFocus(e){this._isDestroyed||Qe(()=>{let i=this._elementRef.nativeElement;switch(this._config.autoFocus){case!1:case"dialog":this._containsFocus()||i.focus(e);break;case!0:case"first-tabbable":this._focusTrap?.focusInitialElement(e)||this._focusDialogContainer(e);break;case"first-heading":this._focusByCssSelector('h1, h2, h3, h4, h5, h6, [role="heading"]',e);break;default:this._focusByCssSelector(this._config.autoFocus,e);break}this._focusTrapped.next()},{injector:this._injector})}_restoreFocus(){let e=this._config.restoreFocus,i=null;if(typeof e=="string"?i=this._document.querySelector(e):typeof e=="boolean"?i=e?this._elementFocusedBeforeDialogWasOpened:null:e&&(i=e),this._config.restoreFocus&&i&&typeof i.focus=="function"){let r=ml(),o=this._elementRef.nativeElement;(!r||r===this._document.body||r===o||o.contains(r))&&(this._focusMonitor?(this._focusMonitor.focusVia(i,this._closeInteractionType),this._closeInteractionType=null):i.focus())}this._focusTrap&&this._focusTrap.destroy()}_focusDialogContainer(e){this._elementRef.nativeElement.focus?.(e)}_containsFocus(){let e=this._elementRef.nativeElement,i=ml();return e===i||e.contains(i)}_initializeFocusTrap(){this._platform.isBrowser&&(this._focusTrap=this._focusTrapFactory.create(this._elementRef.nativeElement),this._document&&(this._elementFocusedBeforeDialogWasOpened=ml()))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["cdk-dialog-container"]],viewQuery:function(i,r){if(i&1&&Ue(Gn,7),i&2){let o;ce(o=de())&&(r._portalOutlet=o.first)}},hostAttrs:["tabindex","-1",1,"cdk-dialog-container"],hostVars:6,hostBindings:function(i,r){i&2&&re("id",r._config.id||null)("role",r._config.role)("aria-modal",r._config.ariaModal)("aria-labelledby",r._config.ariaLabel?null:r._ariaLabelledByQueue[0])("aria-label",r._config.ariaLabel)("aria-describedby",r._config.ariaDescribedBy||null)},features:[Me],decls:1,vars:0,consts:[["cdkPortalOutlet",""]],template:function(i,r){i&1&&Dt(0,lP,0,0,"ng-template",0)},dependencies:[Gn],styles:[`.cdk-dialog-container {
  display: block;
  width: 100%;
  height: 100%;
  min-height: inherit;
  max-height: inherit;
}
`],encapsulation:2,changeDetection:1})}return t})(),ho=class{overlayRef;config;componentInstance=null;componentRef=null;containerInstance;disableClose;closed=new E;backdropClick;keydownEvents;outsidePointerEvents;id;_detachSubscription;constructor(n,e){this.overlayRef=n,this.config=e,this.disableClose=e.disableClose,this.backdropClick=n.backdropClick(),this.keydownEvents=n.keydownEvents(),this.outsidePointerEvents=n.outsidePointerEvents(),this.id=e.id,this.keydownEvents.subscribe(i=>{i.keyCode===27&&!this.disableClose&&!$e(i)&&(i.preventDefault(),this.close(void 0,{focusOrigin:"keyboard"}))}),this.backdropClick.subscribe(()=>{!this.disableClose&&this._canClose()?this.close(void 0,{focusOrigin:"mouse"}):this.containerInstance._recaptureFocus?.()}),this._detachSubscription=n.detachments().subscribe(()=>{e.closeOnOverlayDetachments!==!1&&this.close()})}close(n,e){if(this._canClose(n)){let i=this.closed;this.containerInstance._closeInteractionType=e?.focusOrigin||"program",this._detachSubscription.unsubscribe(),this.overlayRef.dispose(),i.next(n),i.complete(),this.componentInstance=this.containerInstance=null}}updatePosition(){return this.overlayRef.updatePosition(),this}updateSize(n="",e=""){return this.overlayRef.updateSize({width:n,height:e}),this}addPanelClass(n){return this.overlayRef.addPanelClass(n),this}removePanelClass(n){return this.overlayRef.removePanelClass(n),this}_canClose(n){let e=this.config;return!!this.containerInstance&&(!e.closePredicate||e.closePredicate(n,e,this.componentInstance))}},cP=new b("DialogScrollStrategy",{providedIn:"root",factory:()=>{let t=d(R);return()=>rr(t)}}),dP=new b("DialogData"),uP=new b("DefaultDialogConfig");function fP(t){let n=j(t),e=new W;return{valueSignal:n,get value(){return n()},change:e,ngOnDestroy(){e.complete()}}}var Cl=(()=>{class t{_injector=d(R);_defaultOptions=d(uP,{optional:!0});_parentDialog=d(t,{optional:!0,skipSelf:!0});_overlayContainer=d(Ku);_idGenerator=d(xe);_openDialogsAtThisLevel=[];_afterAllClosedAtThisLevel=new E;_afterOpenedAtThisLevel=new E;_ariaHiddenElements=new Map;_scrollStrategy=d(cP);get openDialogs(){return this._parentDialog?this._parentDialog.openDialogs:this._openDialogsAtThisLevel}get afterOpened(){return this._parentDialog?this._parentDialog.afterOpened:this._afterOpenedAtThisLevel}afterAllClosed=cn(()=>this.openDialogs.length?this._getAfterAllClosed():this._getAfterAllClosed().pipe(nt(void 0)));open(e,i){let r=this._defaultOptions||new or;i=v(v({},r),i),i.id=i.id||this._idGenerator.getId("cdk-dialog-"),i.id&&this.getDialogById(i.id);let o=this._getOverlayConfig(i),s=In(this._injector,o),a=new ho(s,i),l=this._attachContainer(s,a,i);if(a.containerInstance=l,!this.openDialogs.length){let c=this._overlayContainer.getContainerElement();l._focusTrapped?l._focusTrapped.pipe(Ae(1)).subscribe(()=>{this._hideNonDialogContentFromAssistiveTechnology(c)}):this._hideNonDialogContentFromAssistiveTechnology(c)}return this._attachDialogContent(e,a,l,i),this.openDialogs.push(a),a.closed.subscribe(()=>this._removeOpenDialog(a,!0)),this.afterOpened.next(a),a}closeAll(){rv(this.openDialogs,e=>e.close())}getDialogById(e){return this.openDialogs.find(i=>i.id===e)}ngOnDestroy(){rv(this._openDialogsAtThisLevel,e=>{e.config.closeOnDestroy===!1&&this._removeOpenDialog(e,!1)}),rv(this._openDialogsAtThisLevel,e=>e.close()),this._afterAllClosedAtThisLevel.complete(),this._afterOpenedAtThisLevel.complete(),this._openDialogsAtThisLevel=[]}_getOverlayConfig(e){let i=new wn({positionStrategy:e.positionStrategy||Wn().centerHorizontally().centerVertically(),scrollStrategy:e.scrollStrategy||this._scrollStrategy(),panelClass:e.panelClass,hasBackdrop:e.hasBackdrop,direction:e.direction,minWidth:e.minWidth,minHeight:e.minHeight,maxWidth:e.maxWidth,maxHeight:e.maxHeight,width:e.width,height:e.height,disposeOnNavigation:e.closeOnNavigation,disableAnimations:e.disableAnimations});return e.backdropClass&&(i.backdropClass=e.backdropClass),i}_attachContainer(e,i,r){let o=r.injector||r.viewContainerRef?.injector,s=[{provide:or,useValue:r},{provide:ho,useValue:i},{provide:bs,useValue:e}],a;r.container?typeof r.container=="function"?a=r.container:(a=r.container.type,s.push(...r.container.providers(r))):a=Sl;let l=new Cn(a,r.viewContainerRef,R.create({parent:o||this._injector,providers:s}));return e.attach(l).instance}_attachDialogContent(e,i,r,o){if(e instanceof Ct){let s=this._createInjector(o,i,r,void 0),a={$implicit:o.data,dialogRef:i};o.templateContext&&(a=v(v({},a),typeof o.templateContext=="function"?o.templateContext():o.templateContext)),r.attachTemplatePortal(new Dn(e,null,a,s))}else{let s=this._createInjector(o,i,r,this._injector),a=r.attachComponentPortal(new Cn(e,o.viewContainerRef,s,null,o.bindings));i.componentRef=a,i.componentInstance=a.instance}}_createInjector(e,i,r,o){let s=e.injector||e.viewContainerRef?.injector,a=[{provide:dP,useValue:e.data},{provide:ho,useValue:i}];return e.providers&&(typeof e.providers=="function"?a.push(...e.providers(i,e,r)):a.push(...e.providers)),e.direction&&(!s||!s.get(ct,null,{optional:!0}))&&a.push({provide:ct,useValue:fP(e.direction)}),R.create({parent:s||o,providers:a})}_removeOpenDialog(e,i){let r=this.openDialogs.indexOf(e);r>-1&&(this.openDialogs.splice(r,1),this.openDialogs.length||(this._ariaHiddenElements.forEach((o,s)=>{o?s.setAttribute("aria-hidden",o):s.removeAttribute("aria-hidden")}),this._ariaHiddenElements.clear(),i&&this._getAfterAllClosed().next()))}_hideNonDialogContentFromAssistiveTechnology(e){if(e.parentElement){let i=e.parentElement.children;for(let r=i.length-1;r>-1;r--){let o=i[r];o!==e&&o.nodeName!=="SCRIPT"&&o.nodeName!=="STYLE"&&!o.hasAttribute("aria-live")&&!o.hasAttribute("popover")&&(this._ariaHiddenElements.set(o,o.getAttribute("aria-hidden")),o.setAttribute("aria-hidden","true"))}}}_getAfterAllClosed(){let e=this._parentDialog;return e?e._getAfterAllClosed():this._afterAllClosedAtThisLevel}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();function rv(t,n){let e=t.length;for(;e--;)n(t[e])}var ov=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({providers:[Cl],imports:[on,vi,bl,vi]})}return t})();var Ci={XSmall:"(max-width: 599.98px)",Small:"(min-width: 600px) and (max-width: 959.98px)",Medium:"(min-width: 960px) and (max-width: 1279.98px)",Large:"(min-width: 1280px) and (max-width: 1919.98px)",XLarge:"(min-width: 1920px)",Handset:"(max-width: 599.98px) and (orientation: portrait), (max-width: 959.98px) and (orientation: landscape)",Tablet:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait), (min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",Web:"(min-width: 840px) and (orientation: portrait), (min-width: 1280px) and (orientation: landscape)",HandsetPortrait:"(max-width: 599.98px) and (orientation: portrait)",TabletPortrait:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait)",WebPortrait:"(min-width: 840px) and (orientation: portrait)",HandsetLandscape:"(max-width: 959.98px) and (orientation: landscape)",TabletLandscape:"(min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",WebLandscape:"(min-width: 1280px) and (orientation: landscape)"};var hP=new b("MATERIAL_ANIMATIONS"),BD=null;function mP(){return d(hP,{optional:!0})?.animationsDisabled||d(pa,{optional:!0})==="NoopAnimations"?"di-disabled":(BD??=d(gs).matchMedia("(prefers-reduced-motion)").matches,BD?"reduced-motion":"enabled")}function Ce(){return mP()!=="enabled"}function pP(t,n){}var jD="_mat-bottom-sheet-enter",HD="_mat-bottom-sheet-exit",gP=(()=>{class t extends Sl{_breakpointSubscription;_animationsDisabled=Ce();_animationState="void";_animationStateChanged=new W;_destroyed=!1;constructor(){super();let e=d(lo);this._breakpointSubscription=e.observe([Ci.Medium,Ci.Large,Ci.XLarge]).subscribe(()=>{let i=this._elementRef.nativeElement.classList;i.toggle("mat-bottom-sheet-container-medium",e.isMatched(Ci.Medium)),i.toggle("mat-bottom-sheet-container-large",e.isMatched(Ci.Large)),i.toggle("mat-bottom-sheet-container-xlarge",e.isMatched(Ci.XLarge))})}enter(){this._destroyed||(this._animationState="visible",this._changeDetectorRef.markForCheck(),this._changeDetectorRef.detectChanges(),this._animationsDisabled&&this._simulateAnimation(jD))}exit(){this._destroyed||(this._elementRef.nativeElement.setAttribute("mat-exit",""),this._animationState="hidden",this._changeDetectorRef.markForCheck(),this._animationsDisabled&&this._simulateAnimation(HD))}ngOnDestroy(){super.ngOnDestroy(),this._breakpointSubscription.unsubscribe(),this._destroyed=!0}_simulateAnimation(e){this._ngZone.run(()=>{this._handleAnimationEvent(!0,e,this._elementRef.nativeElement),setTimeout(()=>this._handleAnimationEvent(!1,e,this._elementRef.nativeElement))})}_trapFocus(){super._trapFocus({preventScroll:!0})}_handleAnimationEvent(e,i,r){if(r===this._elementRef.nativeElement){let o=i===jD;(o||i===HD)&&this._animationStateChanged.emit({toState:o?"visible":"hidden",phase:e?"start":"done"})}}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-bottom-sheet-container"]],hostAttrs:["tabindex","-1",1,"mat-bottom-sheet-container"],hostVars:9,hostBindings:function(i,r){i&1&&D("animationstart",function(s){return r._handleAnimationEvent(!0,s.animationName,s.target)})("animationend",function(s){return r._handleAnimationEvent(!1,s.animationName,s.target)})("animationcancel",function(s){return r._handleAnimationEvent(!1,s.animationName,s.target)}),i&2&&(re("role",r._config.role)("aria-modal",r._config.ariaModal)("aria-label",r._config.ariaLabel),z("mat-bottom-sheet-container-animations-enabled",!r._animationsDisabled)("mat-bottom-sheet-container-enter",r._animationState==="visible")("mat-bottom-sheet-container-exit",r._animationState==="hidden"))},features:[Me],decls:1,vars:0,consts:[["cdkPortalOutlet",""]],template:function(i,r){i&1&&Dt(0,pP,0,0,"ng-template",0)},dependencies:[Gn],styles:[`@keyframes _mat-bottom-sheet-enter {
  from {
    transform: translateY(100%);
  }
  to {
    transform: none;
  }
}
@keyframes _mat-bottom-sheet-exit {
  from {
    transform: none;
  }
  to {
    transform: translateY(100%);
  }
}
.mat-bottom-sheet-container {
  box-shadow: 0px 8px 10px -5px rgba(0, 0, 0, 0.2), 0px 16px 24px 2px rgba(0, 0, 0, 0.14), 0px 6px 30px 5px rgba(0, 0, 0, 0.12);
  padding: 8px 16px;
  min-width: 100vw;
  box-sizing: border-box;
  display: block;
  outline: 0;
  max-height: 80vh;
  overflow: auto;
  position: relative;
  background: var(--%NS%mat-bottom-sheet-container-background-color, var(--%NS%mat-sys-surface-container-low));
  color: var(--%NS%mat-bottom-sheet-container-text-color, var(--%NS%mat-sys-on-surface));
  font-family: var(--%NS%mat-bottom-sheet-container-text-font, var(--%NS%mat-sys-body-large-font));
  font-size: var(--%NS%mat-bottom-sheet-container-text-size, var(--%NS%mat-sys-body-large-size));
  line-height: var(--%NS%mat-bottom-sheet-container-text-line-height, var(--%NS%mat-sys-body-large-line-height));
  font-weight: var(--%NS%mat-bottom-sheet-container-text-weight, var(--%NS%mat-sys-body-large-weight));
  letter-spacing: var(--%NS%mat-bottom-sheet-container-text-tracking, var(--%NS%mat-sys-body-large-tracking));
}
@media (forced-colors: active) {
  .mat-bottom-sheet-container {
    outline: 1px solid;
  }
}

.mat-bottom-sheet-container-animations-enabled {
  transform: translateY(100%);
}
.mat-bottom-sheet-container-animations-enabled.mat-bottom-sheet-container-enter {
  animation: _mat-bottom-sheet-enter 195ms cubic-bezier(0, 0, 0.2, 1) forwards;
}
.mat-bottom-sheet-container-animations-enabled.mat-bottom-sheet-container-exit {
  animation: _mat-bottom-sheet-exit 375ms cubic-bezier(0.4, 0, 1, 1) backwards;
}

.mat-bottom-sheet-container-xlarge, .mat-bottom-sheet-container-large, .mat-bottom-sheet-container-medium {
  border-top-left-radius: var(--%NS%mat-bottom-sheet-container-shape, 28px);
  border-top-right-radius: var(--%NS%mat-bottom-sheet-container-shape, 28px);
}

.mat-bottom-sheet-container-medium {
  min-width: 384px;
  max-width: calc(100vw - 128px);
}

.mat-bottom-sheet-container-large {
  min-width: 512px;
  max-width: calc(100vw - 256px);
}

.mat-bottom-sheet-container-xlarge {
  min-width: 576px;
  max-width: calc(100vw - 384px);
}
`],encapsulation:2,changeDetection:1})}return t})(),av=new b("MatBottomSheetData"),sv=class{viewContainerRef;injector;panelClass;direction;data=null;hasBackdrop=!0;backdropClass;disableClose=!1;ariaLabel=null;ariaModal=!1;closeOnNavigation=!0;autoFocus="first-tabbable";restoreFocus=!0;scrollStrategy;height="";minHeight;maxHeight;bindings},ys=class{_ref;get instance(){return this._ref.componentInstance}get componentRef(){return this._ref.componentRef}containerInstance;disableClose;_afterOpened=new E;_result;_closeFallbackTimeout;constructor(n,e,i){this._ref=n,this.containerInstance=i,this.disableClose=e.disableClose,i._animationStateChanged.pipe(ge(r=>r.phase==="done"&&r.toState==="visible"),Ae(1)).subscribe(()=>{this._afterOpened.next(),this._afterOpened.complete()}),i._animationStateChanged.pipe(ge(r=>r.phase==="done"&&r.toState==="hidden"),Ae(1)).subscribe(()=>{clearTimeout(this._closeFallbackTimeout),this._ref.close(this._result)}),n.overlayRef.detachments().subscribe(()=>{this._ref.close(this._result)}),Nt(this.backdropClick(),this.keydownEvents().pipe(ge(r=>r.keyCode===27))).subscribe(r=>{!this.disableClose&&(r.type!=="keydown"||!$e(r))&&(r.preventDefault(),this.dismiss())})}dismiss(n){this.containerInstance&&(this.containerInstance._animationStateChanged.pipe(ge(e=>e.phase==="start"),Ae(1)).subscribe(()=>{this._closeFallbackTimeout=setTimeout(()=>this._ref.close(this._result),500),this._ref.overlayRef.detachBackdrop()}),this._result=n,this.containerInstance.exit(),this.containerInstance=null)}afterDismissed(){return this._ref.closed}afterOpened(){return this._afterOpened}backdropClick(){return this._ref.backdropClick}keydownEvents(){return this._ref.keydownEvents}},vP=new b("mat-bottom-sheet-default-options"),UD=(()=>{class t{_injector=d(R);_parentBottomSheet=d(t,{optional:!0,skipSelf:!0});_animationsDisabled=Ce();_defaultOptions=d(vP,{optional:!0});_bottomSheetRefAtThisLevel=null;_dialog=d(Cl);get _openedBottomSheetRef(){let e=this._parentBottomSheet;return e?e._openedBottomSheetRef:this._bottomSheetRefAtThisLevel}set _openedBottomSheetRef(e){this._parentBottomSheet?this._parentBottomSheet._openedBottomSheetRef=e:this._bottomSheetRefAtThisLevel=e}open(e,i){let r=v(v({},this._defaultOptions||new sv),i),o;return this._dialog.open(e,k(v({},r),{disableClose:!0,closeOnOverlayDetachments:!1,maxWidth:"100%",container:gP,scrollStrategy:r.scrollStrategy||rr(this._injector),positionStrategy:Wn(this._injector).centerHorizontally().bottom("0"),disableAnimations:this._animationsDisabled,templateContext:()=>({bottomSheetRef:o}),providers:(s,a,l)=>(o=new ys(s,r,l),[{provide:ys,useValue:o},{provide:av,useValue:r.data}])})),o.afterDismissed().subscribe(()=>{this._openedBottomSheetRef===o&&(this._openedBottomSheetRef=null)}),this._openedBottomSheetRef?(this._openedBottomSheetRef.afterDismissed().subscribe(()=>o.containerInstance?.enter()),this._openedBottomSheetRef.dismiss()):o.containerInstance.enter(),this._openedBottomSheetRef=o,o}dismiss(e){this._openedBottomSheetRef&&this._openedBottomSheetRef.dismiss(e)}ngOnDestroy(){this._bottomSheetRefAtThisLevel&&this._bottomSheetRefAtThisLevel.dismiss()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();var bP=200,Ju=class{_letterKeyStream=new E;_items=[];_selectedItemIndex=-1;_pressedLetters=[];_skipPredicateFn;_selectedItem=new E;selectedItem=this._selectedItem;constructor(n,e){let i=typeof e?.debounceInterval=="number"?e.debounceInterval:bP;e?.skipPredicate&&(this._skipPredicateFn=e.skipPredicate),this.setItems(n),this._setupKeyHandler(i)}destroy(){this._pressedLetters=[],this._letterKeyStream.complete(),this._selectedItem.complete()}setCurrentSelectedItemIndex(n){this._selectedItemIndex=n}setItems(n){this._items=n}handleKey(n){let e=n.keyCode;n.key&&n.key.length===1?this._letterKeyStream.next(n.key.toLocaleUpperCase()):(e>=65&&e<=90||e>=48&&e<=57)&&this._letterKeyStream.next(String.fromCharCode(e))}isTyping(){return this._pressedLetters.length>0}reset(){this._pressedLetters=[]}_setupKeyHandler(n){this._letterKeyStream.pipe(dt(e=>this._pressedLetters.push(e)),Xs(n),ge(()=>this._pressedLetters.length>0),oe(()=>this._pressedLetters.join("").toLocaleUpperCase())).subscribe(e=>{for(let i=1;i<this._items.length+1;i++){let r=(this._selectedItemIndex+i)%this._items.length,o=this._items[r];if(!this._skipPredicateFn?.(o)&&o.getLabel?.().toLocaleUpperCase().trim().indexOf(e)===0){this._selectedItem.next(o);break}}this._pressedLetters=[]})}};var Ss=class{_items;_activeItemIndex=j(-1);_activeItem=j(null);_wrap=!1;_typeaheadSubscription=pe.EMPTY;_itemChangesSubscription;_vertical=!0;_horizontal=null;_allowedModifierKeys=[];_homeAndEnd=!1;_pageUpAndDown={enabled:!1,delta:10};_effectRef;_typeahead;_skipPredicateFn=n=>n.disabled;constructor(n,e){this._items=n,n instanceof ai?this._itemChangesSubscription=n.changes.subscribe(i=>this._itemsChanged(i.toArray())):Zt(n)&&(this._effectRef=ut(()=>this._itemsChanged(n()),{injector:e}))}tabOut=new E;change=new E;skipPredicate(n){return this._skipPredicateFn=n,this}withWrap(n=!0){return this._wrap=n,this}withVerticalOrientation(n=!0){return this._vertical=n,this}withHorizontalOrientation(n){return this._horizontal=n,this}withAllowedModifierKeys(n){return this._allowedModifierKeys=n,this}withTypeAhead(n=200){this._typeaheadSubscription.unsubscribe();let e=this._getItemsArray();return this._typeahead=new Ju(e,{debounceInterval:typeof n=="number"?n:void 0,skipPredicate:i=>this._skipPredicateFn(i)}),this._typeaheadSubscription=this._typeahead.selectedItem.subscribe(i=>{this.setActiveItem(i)}),this}cancelTypeahead(){return this._typeahead?.reset(),this}withHomeAndEnd(n=!0){return this._homeAndEnd=n,this}withPageUpDown(n=!0,e=10){return this._pageUpAndDown={enabled:n,delta:e},this}setActiveItem(n){let e=this._activeItem();this.updateActiveItem(n),this._activeItem()!==e&&this.change.next(this._activeItemIndex())}onKeydown(n){let e=n.keyCode,r=["altKey","ctrlKey","metaKey","shiftKey"].every(o=>!n[o]||this._allowedModifierKeys.indexOf(o)>-1);switch(e){case 9:this.tabOut.next();return;case 40:if(this._vertical&&r){this.setNextItemActive();break}else return;case 38:if(this._vertical&&r){this.setPreviousItemActive();break}else return;case 39:if(this._horizontal&&r){this._horizontal==="rtl"?this.setPreviousItemActive():this.setNextItemActive();break}else return;case 37:if(this._horizontal&&r){this._horizontal==="rtl"?this.setNextItemActive():this.setPreviousItemActive();break}else return;case 36:if(this._homeAndEnd&&r){this.setFirstItemActive();break}else return;case 35:if(this._homeAndEnd&&r){this.setLastItemActive();break}else return;case 33:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()-this._pageUpAndDown.delta;this._setActiveItemByIndex(o>0?o:0,1);break}else return;case 34:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()+this._pageUpAndDown.delta,s=this._getItemsArray().length;this._setActiveItemByIndex(o<s?o:s-1,-1);break}else return;default:(r||$e(n,"shiftKey"))&&this._typeahead?.handleKey(n);return}this._typeahead?.reset(),n.preventDefault()}get activeItemIndex(){return this._activeItemIndex()}get activeItem(){return this._activeItem()}isTyping(){return!!this._typeahead&&this._typeahead.isTyping()}setFirstItemActive(){this._setActiveItemByIndex(0,1)}setLastItemActive(){this._setActiveItemByIndex(this._getItemsArray().length-1,-1)}setNextItemActive(){this._activeItemIndex()<0?this.setFirstItemActive():this._setActiveItemByDelta(1)}setPreviousItemActive(){this._activeItemIndex()<0&&this._wrap?this.setLastItemActive():this._setActiveItemByDelta(-1)}updateActiveItem(n){let e=this._getItemsArray(),i=typeof n=="number"?n:e.indexOf(n),r=e[i];this._activeItem.set(r??null),this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i)}destroy(){this._typeaheadSubscription.unsubscribe(),this._itemChangesSubscription?.unsubscribe(),this._effectRef?.destroy(),this._typeahead?.destroy(),this.tabOut.complete(),this.change.complete()}_setActiveItemByDelta(n){this._wrap?this._setActiveInWrapMode(n):this._setActiveInDefaultMode(n)}_setActiveInWrapMode(n){let e=this._getItemsArray();for(let i=1;i<=e.length;i++){let r=(this._activeItemIndex()+n*i+e.length)%e.length,o=e[r];if(!this._skipPredicateFn(o)){this.setActiveItem(r);return}}}_setActiveInDefaultMode(n){this._setActiveItemByIndex(this._activeItemIndex()+n,n)}_setActiveItemByIndex(n,e){let i=this._getItemsArray();if(i[n]){for(;this._skipPredicateFn(i[n]);)if(n+=e,!i[n])return;this.setActiveItem(n)}}_getItemsArray(){return Zt(this._items)?this._items():this._items instanceof ai?this._items.toArray():this._items}_itemsChanged(n){this._typeahead?.setItems(n);let e=this._activeItem();if(e){let i=n.indexOf(e);i>-1&&i!==this._activeItemIndex()&&(this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i))}}};var Dl=class extends Ss{setActiveItem(n){this.activeItem&&this.activeItem.setInactiveStyles(),super.setActiveItem(n),this.activeItem&&this.activeItem.setActiveStyles()}};var El=class extends Ss{_origin="program";setFocusOrigin(n){return this._origin=n,this}setActiveItem(n){super.setActiveItem(n),this.activeItem&&this.activeItem.focus(this._origin)}};var GD=" ";function _P(t,n,e){let i=tf(t,n);e=e.trim(),!i.some(r=>r.trim()===e)&&(i.push(e),t.setAttribute(n,i.join(GD)))}function yP(t,n,e){let i=tf(t,n);e=e.trim();let r=i.filter(o=>o!==e);r.length?t.setAttribute(n,r.join(GD)):t.removeAttribute(n)}function tf(t,n){return t.getAttribute(n)?.match(/\S+/g)??[]}var WD="cdk-describedby-message",ef="cdk-describedby-host",cv=0,qD=(()=>{class t{_platform=d(be);_document=d(L);_messageRegistry=new Map;_messagesContainer=null;_id=`${cv++}`;constructor(){d(ze).load(ms),this._id=d(si)+"-"+cv++}describe(e,i,r){if(!this._canBeDescribed(e,i))return;let o=lv(i,r);typeof i!="string"?($D(i,this._id),this._messageRegistry.set(o,{messageElement:i,referenceCount:0})):this._messageRegistry.has(o)||this._createMessageElement(i,r),this._isElementDescribedByMessage(e,o)||this._addMessageReference(e,o)}removeDescription(e,i,r){if(!i||!this._isElementNode(e))return;let o=lv(i,r);if(this._isElementDescribedByMessage(e,o)&&this._removeMessageReference(e,o),typeof i=="string"){let s=this._messageRegistry.get(o);s&&s.referenceCount===0&&this._deleteMessageElement(o)}this._messagesContainer?.childNodes.length===0&&(this._messagesContainer.remove(),this._messagesContainer=null)}ngOnDestroy(){let e=this._document.querySelectorAll(`[${ef}="${this._id}"]`);for(let i=0;i<e.length;i++)this._removeCdkDescribedByReferenceIds(e[i]),e[i].removeAttribute(ef);this._messagesContainer?.remove(),this._messagesContainer=null,this._messageRegistry.clear()}_createMessageElement(e,i){let r=this._document.createElement("div");$D(r,this._id),r.textContent=e,i&&r.setAttribute("role",i),this._createMessagesContainer(),this._messagesContainer.appendChild(r),this._messageRegistry.set(lv(e,i),{messageElement:r,referenceCount:0})}_deleteMessageElement(e){this._messageRegistry.get(e)?.messageElement?.remove(),this._messageRegistry.delete(e)}_createMessagesContainer(){if(this._messagesContainer)return;let e="cdk-describedby-message-container",i=this._document.querySelectorAll(`.${e}[platform="server"]`);for(let o=0;o<i.length;o++)i[o].remove();let r=this._document.createElement("div");r.style.visibility="hidden",r.classList.add(e),r.classList.add("cdk-visually-hidden"),this._platform.isBrowser||r.setAttribute("platform","server"),this._document.body.appendChild(r),this._messagesContainer=r}_removeCdkDescribedByReferenceIds(e){let i=tf(e,"aria-describedby").filter(r=>r.indexOf(WD)!=0);e.setAttribute("aria-describedby",i.join(" "))}_addMessageReference(e,i){let r=this._messageRegistry.get(i);_P(e,"aria-describedby",r.messageElement.id),e.setAttribute(ef,this._id),r.referenceCount++}_removeMessageReference(e,i){let r=this._messageRegistry.get(i);r.referenceCount--,yP(e,"aria-describedby",r.messageElement.id),e.removeAttribute(ef)}_isElementDescribedByMessage(e,i){let r=tf(e,"aria-describedby"),o=this._messageRegistry.get(i),s=o&&o.messageElement.id;return!!s&&r.indexOf(s)!=-1}_canBeDescribed(e,i){if(!this._isElementNode(e))return!1;if(i&&typeof i=="object")return!0;let r=i==null?"":`${i}`.trim(),o=e.getAttribute("aria-label");return r?!o||o.trim()!==r:!1}_isElementNode(e){return e.nodeType===this._document.ELEMENT_NODE}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();function lv(t,n){return typeof t=="string"?`${n||""}/${t}`:t}function $D(t,n){t.id||(t.id=`${WD}-${n}-${cv++}`)}var Cs,YD=["color","button","checkbox","date","datetime-local","email","file","hidden","image","month","number","password","radio","range","reset","search","submit","tel","text","time","url","week"];function dv(){if(Cs)return Cs;if(typeof document!="object"||!document)return Cs=new Set(YD),Cs;let t=document.createElement("input");return Cs=new Set(YD.filter(n=>(t.setAttribute("type",n),t.type===n))),Cs}function sn(t){return t!=null&&`${t}`!="false"}var an=(function(t){return t[t.FADING_IN=0]="FADING_IN",t[t.VISIBLE=1]="VISIBLE",t[t.FADING_OUT=2]="FADING_OUT",t[t.HIDDEN=3]="HIDDEN",t})(an||{}),uv=class{_renderer;element;config;_animationForciblyDisabledThroughCss;state=an.HIDDEN;constructor(n,e,i,r=!1){this._renderer=n,this.element=e,this.config=i,this._animationForciblyDisabledThroughCss=r}fadeOut(){this._renderer.fadeOutRipple(this)}},ZD=hs({passive:!0,capture:!0}),fv=class{_events=new Map;addHandler(n,e,i,r){let o=this._events.get(e);if(o){let s=o.get(i);s?s.add(r):o.set(i,new Set([r]))}else this._events.set(e,new Map([[i,new Set([r])]])),n.runOutsideAngular(()=>{document.addEventListener(e,this._delegateEventHandler,ZD)})}removeHandler(n,e,i){let r=this._events.get(n);if(!r)return;let o=r.get(e);o&&(o.delete(i),o.size===0&&r.delete(e),r.size===0&&(this._events.delete(n),document.removeEventListener(n,this._delegateEventHandler,ZD)))}_delegateEventHandler=n=>{let e=yt(n);e&&this._events.get(n.type)?.forEach((i,r)=>{(r===e||r.contains(e))&&i.forEach(o=>o.handleEvent(n))})}},wl={enterDuration:225,exitDuration:150},SP=800,XD=hs({passive:!0,capture:!0}),KD=["mousedown","touchstart"],QD=["mouseup","mouseleave","touchend","touchcancel"],CP=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["ng-component"]],hostAttrs:["mat-ripple-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.mat-ripple {
  overflow: hidden;
  position: relative;
}
.mat-ripple:not(:empty) {
  transform: translateZ(0);
}

.mat-ripple.mat-ripple-unbounded {
  overflow: visible;
}

.mat-ripple-element {
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
  transition: opacity, transform 0ms cubic-bezier(0, 0, 0.2, 1);
  transform: scale3d(0, 0, 0);
  background-color: var(--%NS%mat-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 10%, transparent));
}
@media (forced-colors: active) {
  .mat-ripple-element {
    display: none;
  }
}
.cdk-drag-preview .mat-ripple-element, .cdk-drag-placeholder .mat-ripple-element {
  display: none;
}
`],encapsulation:2})}return t})(),xl=class t{_target;_ngZone;_platform;_containerElement;_triggerElement=null;_isPointerDown=!1;_activeRipples=new Map;_mostRecentTransientRipple=null;_lastTouchStartEvent;_pointerUpEventsRegistered=!1;_containerRect=null;static _eventManager=new fv;constructor(n,e,i,r,o){this._target=n,this._ngZone=e,this._platform=r,r.isBrowser&&(this._containerElement=$t(i)),o&&o.get(ze).load(CP)}fadeInRipple(n,e,i={}){let r=this._containerRect=this._containerRect||this._containerElement.getBoundingClientRect(),o=v(v({},wl),i.animation);i.centered&&(n=r.left+r.width/2,e=r.top+r.height/2);let s=i.radius||DP(n,e,r),a=n-r.left,l=e-r.top,c=o.enterDuration,u=document.createElement("div");u.classList.add("mat-ripple-element"),u.style.left=`${a-s}px`,u.style.top=`${l-s}px`,u.style.height=`${s*2}px`,u.style.width=`${s*2}px`,i.color!=null&&(u.style.backgroundColor=i.color),u.style.transitionDuration=`${c}ms`,this._containerElement.appendChild(u);let f=window.getComputedStyle(u),h=f.transitionProperty,p=f.transitionDuration,S=h==="none"||p==="0s"||p==="0s, 0s"||r.width===0&&r.height===0,C=new uv(this,u,i,S);u.style.transform="scale3d(1, 1, 1)",C.state=an.FADING_IN,i.persistent||(this._mostRecentTransientRipple=C);let $=null;return!S&&(c||o.exitDuration)&&this._ngZone.runOutsideAngular(()=>{let F=()=>{$&&($.fallbackTimer=null),clearTimeout(Je),this._finishRippleTransition(C)},we=()=>this._destroyRipple(C),Je=setTimeout(we,c+100);u.addEventListener("transitionend",F),u.addEventListener("transitioncancel",we),$={onTransitionEnd:F,onTransitionCancel:we,fallbackTimer:Je}}),this._activeRipples.set(C,$),(S||!c)&&this._finishRippleTransition(C),C}fadeOutRipple(n){if(n.state===an.FADING_OUT||n.state===an.HIDDEN)return;let e=n.element,i=v(v({},wl),n.config.animation);e.style.transitionDuration=`${i.exitDuration}ms`,e.style.opacity="0",n.state=an.FADING_OUT,(n._animationForciblyDisabledThroughCss||!i.exitDuration)&&this._finishRippleTransition(n)}fadeOutAll(){this._getActiveRipples().forEach(n=>n.fadeOut())}fadeOutAllNonPersistent(){this._getActiveRipples().forEach(n=>{n.config.persistent||n.fadeOut()})}setupTriggerEvents(n){let e=$t(n);!this._platform.isBrowser||!e||e===this._triggerElement||(this._removeTriggerEvents(),this._triggerElement=e,KD.forEach(i=>{t._eventManager.addHandler(this._ngZone,i,e,this)}))}handleEvent(n){n.type==="mousedown"?this._onMousedown(n):n.type==="touchstart"?this._onTouchStart(n):this._onPointerUp(),this._pointerUpEventsRegistered||(this._ngZone.runOutsideAngular(()=>{QD.forEach(e=>{this._triggerElement.addEventListener(e,this,XD)})}),this._pointerUpEventsRegistered=!0)}_finishRippleTransition(n){n.state===an.FADING_IN?this._startFadeOutTransition(n):n.state===an.FADING_OUT&&this._destroyRipple(n)}_startFadeOutTransition(n){let e=n===this._mostRecentTransientRipple,{persistent:i}=n.config;n.state=an.VISIBLE,!i&&(!e||!this._isPointerDown)&&n.fadeOut()}_destroyRipple(n){let e=this._activeRipples.get(n)??null;this._activeRipples.delete(n),this._activeRipples.size||(this._containerRect=null),n===this._mostRecentTransientRipple&&(this._mostRecentTransientRipple=null),n.state=an.HIDDEN,e!==null&&(n.element.removeEventListener("transitionend",e.onTransitionEnd),n.element.removeEventListener("transitioncancel",e.onTransitionCancel),e.fallbackTimer!==null&&clearTimeout(e.fallbackTimer)),n.element.remove()}_onMousedown(n){let e=ro(n),i=this._lastTouchStartEvent&&Date.now()<this._lastTouchStartEvent+SP;!this._target.rippleDisabled&&!e&&!i&&(this._isPointerDown=!0,this.fadeInRipple(n.clientX,n.clientY,this._target.rippleConfig))}_onTouchStart(n){if(!this._target.rippleDisabled&&!oo(n)){this._lastTouchStartEvent=Date.now(),this._isPointerDown=!0;let e=n.changedTouches;if(e)for(let i=0;i<e.length;i++)this.fadeInRipple(e[i].clientX,e[i].clientY,this._target.rippleConfig)}}_onPointerUp(){this._isPointerDown&&(this._isPointerDown=!1,this._getActiveRipples().forEach(n=>{let e=n.state===an.VISIBLE||n.config.terminateOnPointerUp&&n.state===an.FADING_IN;!n.config.persistent&&e&&n.fadeOut()}))}_getActiveRipples(){return Array.from(this._activeRipples.keys())}_removeTriggerEvents(){let n=this._triggerElement;n&&(KD.forEach(e=>t._eventManager.removeHandler(e,n,this)),this._pointerUpEventsRegistered&&(QD.forEach(e=>n.removeEventListener(e,this,XD)),this._pointerUpEventsRegistered=!1))}};function DP(t,n,e){let i=Math.max(Math.abs(t-e.left),Math.abs(t-e.right)),r=Math.max(Math.abs(n-e.top),Math.abs(n-e.bottom));return Math.sqrt(i*i+r*r)}var hv=new b("mat-ripple-global-options"),sr=(()=>{class t{_elementRef=d(O);_animationsDisabled=Ce();color;unbounded=!1;centered=!1;radius=0;animation;get disabled(){return this._disabled}set disabled(e){e&&this.fadeOutAllNonPersistent(),this._disabled=e,this._setupTriggerEventsIfEnabled()}_disabled=!1;get trigger(){return this._trigger||this._elementRef.nativeElement}set trigger(e){this._trigger=e,this._setupTriggerEventsIfEnabled()}_trigger;_rippleRenderer;_globalOptions;_isInitialized=!1;constructor(){let e=d(N),i=d(be),r=d(hv,{optional:!0}),o=d(R);this._globalOptions=r||{},this._rippleRenderer=new xl(this,e,this._elementRef,i,o)}ngOnInit(){this._isInitialized=!0,this._setupTriggerEventsIfEnabled()}ngOnDestroy(){this._rippleRenderer._removeTriggerEvents()}fadeOutAll(){this._rippleRenderer.fadeOutAll()}fadeOutAllNonPersistent(){this._rippleRenderer.fadeOutAllNonPersistent()}get rippleConfig(){return{centered:this.centered,radius:this.radius,color:this.color,animation:v(v(v({},this._globalOptions.animation),this._animationsDisabled?{enterDuration:0,exitDuration:0}:{}),this.animation),terminateOnPointerUp:this._globalOptions.terminateOnPointerUp}}get rippleDisabled(){return this.disabled||!!this._globalOptions.disabled}_setupTriggerEventsIfEnabled(){!this.disabled&&this._isInitialized&&this._rippleRenderer.setupTriggerEvents(this.trigger)}launch(e,i=0,r){return typeof e=="number"?this._rippleRenderer.fadeInRipple(e,i,v(v({},this.rippleConfig),r)):this._rippleRenderer.fadeInRipple(0,0,v(v({},this.rippleConfig),e))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,selectors:[["","mat-ripple",""],["","matRipple",""]],hostAttrs:[1,"mat-ripple"],hostVars:2,hostBindings:function(i,r){i&2&&z("mat-ripple-unbounded",r.unbounded)},inputs:{color:[0,"matRippleColor","color"],unbounded:[0,"matRippleUnbounded","unbounded"],centered:[0,"matRippleCentered","centered"],radius:[0,"matRippleRadius","radius"],animation:[0,"matRippleAnimation","animation"],disabled:[0,"matRippleDisabled","disabled"],trigger:[0,"matRippleTrigger","trigger"]},exportAs:["matRipple"]})}return t})();var EP={capture:!0},wP=["focus","mousedown","mouseenter","touchstart"],mv="mat-ripple-loader-uninitialized",pv="mat-ripple-loader-class-name",JD="mat-ripple-loader-centered",nf="mat-ripple-loader-disabled",eE=(()=>{class t{_document=d(L);_animationsDisabled=Ce();_globalRippleOptions=d(hv,{optional:!0});_platform=d(be);_ngZone=d(N);_injector=d(R);_eventCleanups;_hosts=new Map;constructor(){let e=d(Ke).createRenderer(null,null);this._eventCleanups=this._ngZone.runOutsideAngular(()=>wP.map(i=>e.listen(this._document,i,this._onInteraction,EP)))}ngOnDestroy(){let e=this._hosts.keys();for(let i of e)this.destroyRipple(i);this._eventCleanups.forEach(i=>i())}configureRipple(e,i){e.setAttribute(mv,this._globalRippleOptions?.namespace??""),(i.className||!e.hasAttribute(pv))&&e.setAttribute(pv,i.className||""),i.centered&&e.setAttribute(JD,""),i.disabled&&e.setAttribute(nf,"")}setDisabled(e,i){let r=this._hosts.get(e);r?(r.target.rippleDisabled=i,!i&&!r.hasSetUpEvents&&(r.hasSetUpEvents=!0,r.renderer.setupTriggerEvents(e))):i?e.setAttribute(nf,""):e.removeAttribute(nf)}_onInteraction=e=>{let i=yt(e);if(i instanceof HTMLElement){let r=i.closest(`[${mv}="${this._globalRippleOptions?.namespace??""}"]`);r&&this._createRipple(r)}};_createRipple(e){if(!this._document||this._hosts.has(e))return;e.querySelector(".mat-ripple")?.remove();let i=this._document.createElement("span");i.classList.add("mat-ripple",e.getAttribute(pv)),e.append(i);let r=this._globalRippleOptions,o=this._animationsDisabled?0:r?.animation?.enterDuration??wl.enterDuration,s=this._animationsDisabled?0:r?.animation?.exitDuration??wl.exitDuration,a={rippleDisabled:this._animationsDisabled||r?.disabled||e.hasAttribute(nf),rippleConfig:{centered:e.hasAttribute(JD),terminateOnPointerUp:r?.terminateOnPointerUp,animation:{enterDuration:o,exitDuration:s}}},l=new xl(a,this._ngZone,i,this._platform,this._injector),c=!a.rippleDisabled;c&&l.setupTriggerEvents(e),this._hosts.set(e,{target:a,renderer:l,hasSetUpEvents:c}),e.removeAttribute(mv)}destroyRipple(e){let i=this._hosts.get(e);i&&(i.renderer._removeTriggerEvents(),this._hosts.delete(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();var qn=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["structural-styles"]],decls:0,vars:0,template:function(i,r){},styles:[`.mat-focus-indicator {
  position: relative;
}
.mat-focus-indicator::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  box-sizing: border-box;
  pointer-events: none;
  display: var(--%NS%mat-focus-indicator-display, none);
  border-width: var(--%NS%mat-focus-indicator-border-width, 3px);
  border-style: var(--%NS%mat-focus-indicator-border-style, solid);
  border-color: var(--%NS%mat-focus-indicator-border-color, transparent);
  border-radius: var(--%NS%mat-focus-indicator-border-radius, 4px);
}
.mat-focus-indicator:focus-visible::before {
  content: "";
}

@media (forced-colors: active) {
  html {
    --%NS%mat-focus-indicator-display: block;
    --%NS%mat-focus-indicator-fallback-border-style: none;
  }
}
`],encapsulation:2})}return t})();var xP=["*",[["","progressIndicator",""]]],IP=["*","[progressIndicator]"];function MP(t,n){t&1&&(lt(0,"div",1),he(1,1),_t())}var TP=new b("MAT_BUTTON_CONFIG");function tE(t){return t==null?void 0:Yr(t)}var gv=(()=>{class t{_elementRef=d(O);_ngZone=d(N);_animationsDisabled=Ce();_config=d(TP,{optional:!0});_focusMonitor=d(rn);_cleanupClick;_renderer=d(Re);_rippleLoader=d(eE);_isAnchor;_isFab=!1;color;get disableRipple(){return this._disableRipple}set disableRipple(e){this._disableRipple=e,this._updateRippleDisabled()}_disableRipple=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._updateRippleDisabled()}_disabled=!1;ariaDisabled;disabledInteractive;tabIndex;set _tabindex(e){this.tabIndex=e}showProgress=yn(!1,{transform:P});constructor(){d(ze).load(qn);let e=this._elementRef.nativeElement;this._isAnchor=e.tagName==="A",this.disabledInteractive=this._config?.disabledInteractive??!1,this.color=this._config?.color??null,this._rippleLoader?.configureRipple(e,{className:"mat-mdc-button-ripple"})}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0),this._isAnchor&&this._setupAsAnchor()}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement)}focus(e="program",i){e?this._focusMonitor.focusVia(this._elementRef.nativeElement,e,i):this._elementRef.nativeElement.focus(i)}_getAriaDisabled(){return this.ariaDisabled!=null?this.ariaDisabled:this._isAnchor?this.disabled||null:this.disabled&&this.disabledInteractive?!0:null}_getDisabledAttribute(){return this.disabledInteractive||!this.disabled?null:!0}_updateRippleDisabled(){this._rippleLoader?.setDisabled(this._elementRef.nativeElement,this.disableRipple||this.disabled)}_getTabIndex(){return this._isAnchor?this.disabled&&!this.disabledInteractive?-1:this.tabIndex:this.tabIndex}_setupAsAnchor(){this._cleanupClick=this._ngZone.runOutsideAngular(()=>this._renderer.listen(this._elementRef.nativeElement,"click",e=>{this.disabled&&(e.preventDefault(),e.stopImmediatePropagation())}))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,hostAttrs:[1,"mat-mdc-button-base"],hostVars:15,hostBindings:function(i,r){i&2&&(re("disabled",r._getDisabledAttribute())("aria-disabled",r._getAriaDisabled())("tabindex",r._getTabIndex()),wt(r.color?"mat-"+r.color:""),z("mat-mdc-button-progress-indicator-shown",r.showProgress())("mat-mdc-button-disabled",r.disabled)("mat-mdc-button-disabled-interactive",r.disabledInteractive)("mat-unthemed",!r.color)("_mat-animation-noopable",r._animationsDisabled))},inputs:{color:"color",disableRipple:[2,"disableRipple","disableRipple",P],disabled:[2,"disabled","disabled",P],ariaDisabled:[2,"aria-disabled","ariaDisabled",P],disabledInteractive:[2,"disabledInteractive","disabledInteractive",P],tabIndex:[2,"tabIndex","tabIndex",tE],_tabindex:[2,"tabindex","_tabindex",tE],showProgress:[1,"showProgress"]}})}return t})(),bo=(()=>{class t extends gv{constructor(){super(),this._rippleLoader.configureRipple(this._elementRef.nativeElement,{centered:!0})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["button","mat-icon-button",""],["a","mat-icon-button",""],["button","matIconButton",""],["a","matIconButton",""]],hostAttrs:[1,"mdc-icon-button","mat-mdc-icon-button"],exportAs:["matButton","matAnchor"],features:[Me],ngContentSelectors:IP,decls:5,vars:1,consts:[[1,"mat-mdc-button-persistent-ripple","mdc-icon-button__ripple"],[1,"mat-mdc-button-progress-indicator-container"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(Pe(xP),Qt(0,"span",0),he(1),V(2,MP,2,0,"div",1),Qt(3,"span",2)(4,"span",3)),i&2&&(_(2),B(r.showProgress()?2:-1))},styles:[`.mat-mdc-icon-button {
  -webkit-user-select: none;
  user-select: none;
  display: inline-block;
  position: relative;
  box-sizing: border-box;
  border: none;
  outline: none;
  background-color: transparent;
  fill: currentColor;
  text-decoration: none;
  cursor: pointer;
  z-index: 0;
  overflow: visible;
  border-radius: var(--%NS%mat-icon-button-container-shape, var(--%NS%mat-sys-corner-full, 50%));
  flex-shrink: 0;
  text-align: center;
  width: var(--%NS%mat-icon-button-state-layer-size, 40px);
  height: var(--%NS%mat-icon-button-state-layer-size, 40px);
  padding: calc(calc(var(--%NS%mat-icon-button-state-layer-size, 40px) - var(--%NS%mat-icon-button-icon-size, 24px)) / 2);
  font-size: var(--%NS%mat-icon-button-icon-size, 24px);
  color: var(--%NS%mat-icon-button-icon-color, var(--%NS%mat-sys-on-surface-variant));
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-icon-button .mat-mdc-button-ripple,
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple,
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-icon-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-icon-button .mdc-button__label,
.mat-mdc-icon-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-icon-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-icon-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-icon-button .mat-ripple-element {
  background-color: var(--%NS%mat-icon-button-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface-variant) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-icon-button-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-icon-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-icon-button-disabled-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-icon-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-icon-button-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-icon-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-icon-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-icon-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-icon-button-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-mdc-icon-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-icon-button-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-icon-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-icon-button-touch-target-size, 48px);
  display: var(--%NS%mat-icon-button-touch-target-display, block);
  left: 50%;
  width: var(--%NS%mat-icon-button-touch-target-size, 48px);
  transform: translate(-50%, -50%);
}
.mat-mdc-icon-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-icon-button[disabled], .mat-mdc-icon-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-icon-button-disabled-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-icon-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-icon-button img,
.mat-mdc-icon-button svg {
  width: var(--%NS%mat-icon-button-icon-size, 24px);
  height: var(--%NS%mat-icon-button-icon-size, 24px);
  vertical-align: baseline;
}
.mat-mdc-icon-button .mat-mdc-button-progress-indicator-container .mdc-circular-progress__determinate-circle-graphic {
  width: inherit;
  height: inherit;
}
.mat-mdc-icon-button .mat-mdc-button-progress-indicator-container .mdc-circular-progress__indeterminate-circle-graphic {
  height: 100%;
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple {
  border-radius: var(--%NS%mat-icon-button-container-shape, var(--%NS%mat-sys-corner-full, 50%));
}
.mat-mdc-icon-button[hidden] {
  display: none;
}
.mat-mdc-icon-button.mat-unthemed:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-primary:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-accent:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-warn:not(.mdc-ripple-upgraded):focus::before {
  background: transparent;
  opacity: 1;
}

.mat-mdc-button-progress-indicator-container {
  position: absolute;
  inset-inline-start: 0;
  inset-block-start: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  width: 100%;
  height: 100%;
  box-sizing: border-box;
}

.mat-mdc-button-progress-indicator-shown mat-icon {
  visibility: hidden;
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2})}return t})();var ar=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({imports:[ve]})}return t})();var NP=[[["",8,"material-icons",3,"iconPositionEnd",""],["mat-icon",3,"iconPositionEnd",""],["","matButtonIcon","",3,"iconPositionEnd",""]],"*",[["","iconPositionEnd","",8,"material-icons"],["mat-icon","iconPositionEnd",""],["","matButtonIcon","","iconPositionEnd",""]],[["","progressIndicator",""]]],kP=[".material-icons:not([iconPositionEnd]), mat-icon:not([iconPositionEnd]), [matButtonIcon]:not([iconPositionEnd])","*",".material-icons[iconPositionEnd], mat-icon[iconPositionEnd], [matButtonIcon][iconPositionEnd]","[progressIndicator]"];function AP(t,n){t&1&&(lt(0,"div",2),he(1,3),_t())}var nE=new Map([["text",["mat-mdc-button"]],["filled",["mdc-button--unelevated","mat-mdc-unelevated-button"]],["elevated",["mdc-button--raised","mat-mdc-raised-button"]],["outlined",["mdc-button--outlined","mat-mdc-outlined-button"]],["tonal",["mat-tonal-button"]]]),lr=(()=>{class t extends gv{get appearance(){return this._appearance}set appearance(e){this.setAppearance(e||this._config?.defaultAppearance||"text")}_appearance=null;constructor(){super();let e=RP(this._elementRef.nativeElement);e&&this.setAppearance(e)}setAppearance(e){if(e===this._appearance)return;let i=this._elementRef.nativeElement.classList,r=this._appearance?nE.get(this._appearance):null,o=nE.get(e);r&&i.remove(...r),i.add(...o),this._appearance=e}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["button","matButton",""],["a","matButton",""],["button","mat-button",""],["button","mat-raised-button",""],["button","mat-flat-button",""],["button","mat-stroked-button",""],["a","mat-button",""],["a","mat-raised-button",""],["a","mat-flat-button",""],["a","mat-stroked-button",""]],hostAttrs:[1,"mdc-button"],inputs:{appearance:[0,"matButton","appearance"]},exportAs:["matButton","matAnchor"],features:[Me],ngContentSelectors:kP,decls:8,vars:5,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-mdc-button-progress-indicator-container"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(Pe(NP),Qt(0,"span",0),he(1),lt(2,"span",1),he(3,1),_t(),he(4,2),V(5,AP,2,0,"div",2),Qt(6,"span",3)(7,"span",4)),i&2&&(z("mdc-button__ripple",!r._isFab)("mdc-fab__ripple",r._isFab),_(5),B(r.showProgress()?5:-1))},styles:[`.mat-mdc-button-base {
  text-decoration: none;
}
.mat-mdc-button-base .mat-icon {
  min-height: fit-content;
  flex-shrink: 0;
}
@media (hover: none) {
  .mat-mdc-button-base:hover > span.mat-mdc-button-persistent-ripple::before {
    opacity: 0;
  }
}

.mdc-button {
  -webkit-user-select: none;
  user-select: none;
  position: relative;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  min-width: 64px;
  border: none;
  outline: none;
  line-height: inherit;
  -webkit-appearance: none;
  overflow: visible;
  vertical-align: middle;
  background: transparent;
  padding: 0 8px;
}
.mdc-button::-moz-focus-inner {
  padding: 0;
  border: 0;
}
.mdc-button:active {
  outline: none;
}
.mdc-button:hover {
  cursor: pointer;
}
.mdc-button:disabled {
  cursor: default;
  pointer-events: none;
}
.mdc-button[hidden] {
  display: none;
}
.mdc-button .mdc-button__label {
  position: relative;
}

.mat-mdc-button {
  padding: 0 var(--%NS%mat-button-text-horizontal-padding, 12px);
  height: var(--%NS%mat-button-text-container-height, 40px);
  font-family: var(--%NS%mat-button-text-label-text-font, var(--%NS%mat-sys-label-large-font));
  font-size: var(--%NS%mat-button-text-label-text-size, var(--%NS%mat-sys-label-large-size));
  letter-spacing: var(--%NS%mat-button-text-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
  text-transform: var(--%NS%mat-button-text-label-text-transform);
  font-weight: var(--%NS%mat-button-text-label-text-weight, var(--%NS%mat-sys-label-large-weight));
}
.mat-mdc-button, .mat-mdc-button .mdc-button__ripple {
  border-radius: var(--%NS%mat-button-text-container-shape, var(--%NS%mat-sys-corner-full));
}
.mat-mdc-button:not(:disabled) {
  color: var(--%NS%mat-button-text-label-text-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-button[disabled], .mat-mdc-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-button-text-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-button:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding: 0 var(--%NS%mat-button-text-with-icon-horizontal-padding, 16px);
}
.mat-mdc-button > .mat-icon {
  margin-right: var(--%NS%mat-button-text-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-text-icon-offset, -4px);
}
[dir=rtl] .mat-mdc-button > .mat-icon {
  margin-right: var(--%NS%mat-button-text-icon-offset, -4px);
  margin-left: var(--%NS%mat-button-text-icon-spacing, 8px);
}
.mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-text-icon-offset, -4px);
  margin-left: var(--%NS%mat-button-text-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-text-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-text-icon-offset, -4px);
}
.mat-mdc-button .mat-ripple-element {
  background-color: var(--%NS%mat-button-text-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-primary) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-text-state-layer-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-text-disabled-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-text-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-text-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-mdc-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-text-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-button-text-touch-target-size, 48px);
  display: var(--%NS%mat-button-text-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-unelevated-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--%NS%mat-button-filled-container-height, 40px);
  font-family: var(--%NS%mat-button-filled-label-text-font, var(--%NS%mat-sys-label-large-font));
  font-size: var(--%NS%mat-button-filled-label-text-size, var(--%NS%mat-sys-label-large-size));
  letter-spacing: var(--%NS%mat-button-filled-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
  text-transform: var(--%NS%mat-button-filled-label-text-transform);
  font-weight: var(--%NS%mat-button-filled-label-text-weight, var(--%NS%mat-sys-label-large-weight));
  padding: 0 var(--%NS%mat-button-filled-horizontal-padding, 24px);
}
.mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--%NS%mat-button-filled-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-filled-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--%NS%mat-button-filled-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-filled-icon-spacing, 8px);
}
.mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-filled-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-filled-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-filled-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-filled-icon-offset, -8px);
}
.mat-mdc-unelevated-button .mat-ripple-element {
  background-color: var(--%NS%mat-button-filled-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-on-primary) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-filled-state-layer-color, var(--%NS%mat-sys-on-primary));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-filled-disabled-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-unelevated-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-filled-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-unelevated-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-filled-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-mdc-unelevated-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-filled-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-unelevated-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-button-filled-touch-target-size, 48px);
  display: var(--%NS%mat-button-filled-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-unelevated-button:not(:disabled) {
  color: var(--%NS%mat-button-filled-label-text-color, var(--%NS%mat-sys-on-primary));
  background-color: var(--%NS%mat-button-filled-container-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-unelevated-button, .mat-mdc-unelevated-button .mdc-button__ripple {
  border-radius: var(--%NS%mat-button-filled-container-shape, var(--%NS%mat-sys-corner-full));
}
.mat-mdc-unelevated-button .mat-mdc-button-progress-indicator-container {
  --%NS%mat-progress-spinner-active-indicator-color: var(--%NS%mat-button-filled-progress-active-indicator-color, var(--%NS%mat-sys-on-primary));
}
.mat-mdc-unelevated-button[disabled], .mat-mdc-unelevated-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-button-filled-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  background-color: var(--%NS%mat-button-filled-disabled-container-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-raised-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  box-shadow: var(--%NS%mat-button-protected-container-elevation-shadow, var(--%NS%mat-sys-level1));
  height: var(--%NS%mat-button-protected-container-height, 40px);
  font-family: var(--%NS%mat-button-protected-label-text-font, var(--%NS%mat-sys-label-large-font));
  font-size: var(--%NS%mat-button-protected-label-text-size, var(--%NS%mat-sys-label-large-size));
  letter-spacing: var(--%NS%mat-button-protected-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
  text-transform: var(--%NS%mat-button-protected-label-text-transform);
  font-weight: var(--%NS%mat-button-protected-label-text-weight, var(--%NS%mat-sys-label-large-weight));
  padding: 0 var(--%NS%mat-button-protected-horizontal-padding, 24px);
}
.mat-mdc-raised-button > .mat-icon {
  margin-right: var(--%NS%mat-button-protected-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-protected-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-raised-button > .mat-icon {
  margin-right: var(--%NS%mat-button-protected-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-protected-icon-spacing, 8px);
}
.mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-protected-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-protected-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-protected-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-protected-icon-offset, -8px);
}
.mat-mdc-raised-button .mat-ripple-element {
  background-color: var(--%NS%mat-button-protected-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-primary) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-protected-state-layer-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-raised-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-protected-disabled-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-raised-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-protected-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-raised-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-protected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-mdc-raised-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-protected-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-raised-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-button-protected-touch-target-size, 48px);
  display: var(--%NS%mat-button-protected-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-raised-button:not(:disabled) {
  color: var(--%NS%mat-button-protected-label-text-color, var(--%NS%mat-sys-primary));
  background-color: var(--%NS%mat-button-protected-container-color, var(--%NS%mat-sys-surface));
}
.mat-mdc-raised-button, .mat-mdc-raised-button .mdc-button__ripple {
  border-radius: var(--%NS%mat-button-protected-container-shape, var(--%NS%mat-sys-corner-full));
}
@media (hover: hover) {
  .mat-mdc-raised-button:hover {
    box-shadow: var(--%NS%mat-button-protected-hover-container-elevation-shadow, var(--%NS%mat-sys-level2));
  }
}
.mat-mdc-raised-button:focus {
  box-shadow: var(--%NS%mat-button-protected-focus-container-elevation-shadow, var(--%NS%mat-sys-level1));
}
.mat-mdc-raised-button:active, .mat-mdc-raised-button:focus:active {
  box-shadow: var(--%NS%mat-button-protected-pressed-container-elevation-shadow, var(--%NS%mat-sys-level1));
}
.mat-mdc-raised-button[disabled], .mat-mdc-raised-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-button-protected-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  background-color: var(--%NS%mat-button-protected-disabled-container-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-raised-button[disabled].mat-mdc-button-disabled, .mat-mdc-raised-button.mat-mdc-button-disabled.mat-mdc-button-disabled {
  box-shadow: var(--%NS%mat-button-protected-disabled-container-elevation-shadow, var(--%NS%mat-sys-level0));
}
.mat-mdc-raised-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-outlined-button {
  border-style: solid;
  transition: border 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--%NS%mat-button-outlined-container-height, 40px);
  font-family: var(--%NS%mat-button-outlined-label-text-font, var(--%NS%mat-sys-label-large-font));
  font-size: var(--%NS%mat-button-outlined-label-text-size, var(--%NS%mat-sys-label-large-size));
  letter-spacing: var(--%NS%mat-button-outlined-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
  text-transform: var(--%NS%mat-button-outlined-label-text-transform);
  font-weight: var(--%NS%mat-button-outlined-label-text-weight, var(--%NS%mat-sys-label-large-weight));
  border-radius: var(--%NS%mat-button-outlined-container-shape, var(--%NS%mat-sys-corner-full));
  border-width: var(--%NS%mat-button-outlined-outline-width, 1px);
  padding: 0 var(--%NS%mat-button-outlined-horizontal-padding, 24px);
}
.mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--%NS%mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-outlined-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--%NS%mat-button-outlined-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-outlined-icon-spacing, 8px);
}
.mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-outlined-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-outlined-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-outlined-icon-offset, -8px);
}
.mat-mdc-outlined-button .mat-ripple-element {
  background-color: var(--%NS%mat-button-outlined-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-primary) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-outlined-state-layer-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-outlined-disabled-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-outlined-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-outlined-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-outlined-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-outlined-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-mdc-outlined-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-outlined-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-outlined-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-button-outlined-touch-target-size, 48px);
  display: var(--%NS%mat-button-outlined-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-outlined-button:not(:disabled) {
  color: var(--%NS%mat-button-outlined-label-text-color, var(--%NS%mat-sys-primary));
  border-color: var(--%NS%mat-button-outlined-outline-color, var(--%NS%mat-sys-outline));
}
.mat-mdc-outlined-button[disabled], .mat-mdc-outlined-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-button-outlined-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  border-color: var(--%NS%mat-button-outlined-disabled-outline-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-tonal-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--%NS%mat-button-tonal-container-height, 40px);
  font-family: var(--%NS%mat-button-tonal-label-text-font, var(--%NS%mat-sys-label-large-font));
  font-size: var(--%NS%mat-button-tonal-label-text-size, var(--%NS%mat-sys-label-large-size));
  letter-spacing: var(--%NS%mat-button-tonal-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
  text-transform: var(--%NS%mat-button-tonal-label-text-transform);
  font-weight: var(--%NS%mat-button-tonal-label-text-weight, var(--%NS%mat-sys-label-large-weight));
  padding: 0 var(--%NS%mat-button-tonal-horizontal-padding, 24px);
}
.mat-tonal-button:not(:disabled) {
  color: var(--%NS%mat-button-tonal-label-text-color, var(--%NS%mat-sys-on-secondary-container));
  background-color: var(--%NS%mat-button-tonal-container-color, var(--%NS%mat-sys-secondary-container));
}
.mat-tonal-button, .mat-tonal-button .mdc-button__ripple {
  border-radius: var(--%NS%mat-button-tonal-container-shape, var(--%NS%mat-sys-corner-full));
}
.mat-tonal-button[disabled], .mat-tonal-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-button-tonal-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  background-color: var(--%NS%mat-button-tonal-disabled-container-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mat-tonal-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-tonal-button > .mat-icon {
  margin-right: var(--%NS%mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-tonal-icon-offset, -8px);
}
[dir=rtl] .mat-tonal-button > .mat-icon {
  margin-right: var(--%NS%mat-button-tonal-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-tonal-icon-spacing, 8px);
}
.mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-tonal-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-tonal-icon-spacing, 8px);
}
[dir=rtl] .mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-tonal-icon-offset, -8px);
}
.mat-tonal-button .mat-ripple-element {
  background-color: var(--%NS%mat-button-tonal-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-on-secondary-container) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-tonal-state-layer-color, var(--%NS%mat-sys-on-secondary-container));
}
.mat-tonal-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-tonal-disabled-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-tonal-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-tonal-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-tonal-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-tonal-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-tonal-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-tonal-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}
.mat-tonal-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-button-tonal-touch-target-size, 48px);
  display: var(--%NS%mat-button-tonal-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-button,
.mat-mdc-unelevated-button,
.mat-mdc-raised-button,
.mat-mdc-outlined-button,
.mat-tonal-button {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-button .mdc-button__label,
.mat-mdc-button .mat-icon,
.mat-mdc-unelevated-button .mdc-button__label,
.mat-mdc-unelevated-button .mat-icon,
.mat-mdc-raised-button .mdc-button__label,
.mat-mdc-raised-button .mat-icon,
.mat-mdc-outlined-button .mdc-button__label,
.mat-mdc-outlined-button .mat-icon,
.mat-tonal-button .mdc-button__label,
.mat-tonal-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-button .mat-focus-indicator,
.mat-mdc-unelevated-button .mat-focus-indicator,
.mat-mdc-raised-button .mat-focus-indicator,
.mat-mdc-outlined-button .mat-focus-indicator,
.mat-tonal-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-unelevated-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-raised-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-outlined-button:focus-visible > .mat-focus-indicator::before,
.mat-tonal-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-button._mat-animation-noopable,
.mat-mdc-unelevated-button._mat-animation-noopable,
.mat-mdc-raised-button._mat-animation-noopable,
.mat-mdc-outlined-button._mat-animation-noopable,
.mat-tonal-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-button > .mat-icon,
.mat-mdc-unelevated-button > .mat-icon,
.mat-mdc-raised-button > .mat-icon,
.mat-mdc-outlined-button > .mat-icon,
.mat-tonal-button > .mat-icon {
  display: inline-block;
  position: relative;
  vertical-align: top;
  font-size: 1.125rem;
  height: 1.125rem;
  width: 1.125rem;
}

.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mdc-button__ripple {
  top: -1px;
  left: -1px;
  bottom: -1px;
  right: -1px;
}

.mat-mdc-unelevated-button .mat-focus-indicator::before,
.mat-tonal-button .mat-focus-indicator::before,
.mat-mdc-raised-button .mat-focus-indicator::before {
  margin: calc(calc(var(--%NS%mat-focus-indicator-border-width, 3px) + 2px) * -1);
}

.mat-mdc-outlined-button .mat-focus-indicator::before {
  margin: calc(calc(var(--%NS%mat-focus-indicator-border-width, 3px) + 3px) * -1);
}

.mat-mdc-button-progress-indicator-container {
  position: absolute;
  inset-inline-start: 0;
  inset-block-start: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  width: 100%;
  height: 100%;
  box-sizing: border-box;
}

.mat-mdc-button-progress-indicator-shown mat-icon,
.mat-mdc-button-progress-indicator-shown [matButtonIcon],
.mat-mdc-button-progress-indicator-shown .mdc-button__label {
  visibility: hidden;
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2})}return t})();function RP(t){return t.hasAttribute("mat-raised-button")?"elevated":t.hasAttribute("mat-stroked-button")?"outlined":t.hasAttribute("mat-flat-button")?"filled":t.hasAttribute("mat-button")?"text":null}var Mn=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({imports:[ar,ve]})}return t})();var OP=["*"];var PP=new b("MAT_CARD_CONFIG"),iE=(()=>{class t{appearance;constructor(){let e=d(PP,{optional:!0});this.appearance=e?.appearance||"raised"}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-card"]],hostAttrs:[1,"mat-mdc-card","mdc-card"],hostVars:8,hostBindings:function(i,r){i&2&&z("mat-mdc-card-outlined",r.appearance==="outlined")("mdc-card--outlined",r.appearance==="outlined")("mat-mdc-card-filled",r.appearance==="filled")("mdc-card--filled",r.appearance==="filled")},inputs:{appearance:"appearance"},exportAs:["matCard"],ngContentSelectors:OP,decls:1,vars:0,template:function(i,r){i&1&&(Pe(),he(0))},styles:[`.mat-mdc-card {
  display: flex;
  flex-direction: column;
  box-sizing: border-box;
  position: relative;
  border-style: solid;
  border-width: 0;
  background-color: var(--%NS%mat-card-elevated-container-color, var(--%NS%mat-sys-surface-container-low));
  border-color: var(--%NS%mat-card-elevated-container-color, var(--%NS%mat-sys-surface-container-low));
  border-radius: var(--%NS%mat-card-elevated-container-shape, var(--%NS%mat-sys-corner-medium));
  box-shadow: var(--%NS%mat-card-elevated-container-elevation, var(--%NS%mat-sys-level1));
}
.mat-mdc-card::after {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  border: solid 1px transparent;
  content: "";
  display: block;
  pointer-events: none;
  box-sizing: border-box;
  border-radius: var(--%NS%mat-card-elevated-container-shape, var(--%NS%mat-sys-corner-medium));
}

.mat-mdc-card-outlined {
  background-color: var(--%NS%mat-card-outlined-container-color, var(--%NS%mat-sys-surface));
  border-radius: var(--%NS%mat-card-outlined-container-shape, var(--%NS%mat-sys-corner-medium));
  border-width: var(--%NS%mat-card-outlined-outline-width, 1px);
  border-color: var(--%NS%mat-card-outlined-outline-color, var(--%NS%mat-sys-outline-variant));
  box-shadow: var(--%NS%mat-card-outlined-container-elevation, var(--%NS%mat-sys-level0));
}
.mat-mdc-card-outlined::after {
  border: none;
}

.mat-mdc-card-filled {
  background-color: var(--%NS%mat-card-filled-container-color, var(--%NS%mat-sys-surface-container-highest));
  border-radius: var(--%NS%mat-card-filled-container-shape, var(--%NS%mat-sys-corner-medium));
  box-shadow: var(--%NS%mat-card-filled-container-elevation, var(--%NS%mat-sys-level0));
}

.mdc-card__media {
  position: relative;
  box-sizing: border-box;
  background-repeat: no-repeat;
  background-position: center;
  background-size: cover;
}
.mdc-card__media::before {
  display: block;
  content: "";
}
.mdc-card__media:first-child {
  border-top-left-radius: inherit;
  border-top-right-radius: inherit;
}
.mdc-card__media:last-child {
  border-bottom-left-radius: inherit;
  border-bottom-right-radius: inherit;
}

.mat-mdc-card-actions {
  display: flex;
  flex-direction: row;
  align-items: center;
  box-sizing: border-box;
  min-height: 52px;
  padding: 8px;
}

.mat-mdc-card-title {
  font-family: var(--%NS%mat-card-title-text-font, var(--%NS%mat-sys-title-large-font));
  line-height: var(--%NS%mat-card-title-text-line-height, var(--%NS%mat-sys-title-large-line-height));
  font-size: var(--%NS%mat-card-title-text-size, var(--%NS%mat-sys-title-large-size));
  letter-spacing: var(--%NS%mat-card-title-text-tracking, var(--%NS%mat-sys-title-large-tracking));
  font-weight: var(--%NS%mat-card-title-text-weight, var(--%NS%mat-sys-title-large-weight));
}

.mat-mdc-card-subtitle {
  color: var(--%NS%mat-card-subtitle-text-color, var(--%NS%mat-sys-on-surface));
  font-family: var(--%NS%mat-card-subtitle-text-font, var(--%NS%mat-sys-title-medium-font));
  line-height: var(--%NS%mat-card-subtitle-text-line-height, var(--%NS%mat-sys-title-medium-line-height));
  font-size: var(--%NS%mat-card-subtitle-text-size, var(--%NS%mat-sys-title-medium-size));
  letter-spacing: var(--%NS%mat-card-subtitle-text-tracking, var(--%NS%mat-sys-title-medium-tracking));
  font-weight: var(--%NS%mat-card-subtitle-text-weight, var(--%NS%mat-sys-title-medium-weight));
}

.mat-mdc-card-title,
.mat-mdc-card-subtitle {
  display: block;
  margin: 0;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle {
  padding: 16px 16px 0;
}

.mat-mdc-card-header {
  display: flex;
  padding: 16px 16px 0;
}

.mat-mdc-card-content {
  display: block;
  padding: 0 16px;
}
.mat-mdc-card-content:first-child {
  padding-top: 16px;
}
.mat-mdc-card-content:last-child {
  padding-bottom: 16px;
}

.mat-mdc-card-title-group {
  display: flex;
  justify-content: space-between;
  width: 100%;
}

.mat-mdc-card-avatar {
  height: 40px;
  width: 40px;
  border-radius: 50%;
  flex-shrink: 0;
  margin-bottom: 16px;
  object-fit: cover;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title {
  line-height: normal;
}

.mat-mdc-card-sm-image {
  width: 80px;
  height: 80px;
}

.mat-mdc-card-md-image {
  width: 112px;
  height: 112px;
}

.mat-mdc-card-lg-image {
  width: 152px;
  height: 152px;
}

.mat-mdc-card-xl-image {
  width: 240px;
  height: 240px;
}

.mat-mdc-card-subtitle ~ .mat-mdc-card-title,
.mat-mdc-card-title ~ .mat-mdc-card-subtitle,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-title-group .mat-mdc-card-title,
.mat-mdc-card-title-group .mat-mdc-card-subtitle {
  padding-top: 0;
}

.mat-mdc-card-content > :last-child:not(.mat-mdc-card-footer) {
  margin-bottom: 0;
}

.mat-mdc-card-actions-align-end {
  justify-content: flex-end;
}
`],encapsulation:2})}return t})();var rE=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,selectors:[["mat-card-content"]],hostAttrs:[1,"mat-mdc-card-content"]})}return t})();var oE=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({imports:[ve]})}return t})();function LP(t,n){}var of=class{viewContainerRef;injector;id;role="dialog";panelClass="";hasBackdrop=!0;backdropClass="";disableClose=!1;closePredicate;width="";height="";minWidth;minHeight;maxWidth;maxHeight;position;data=null;direction;ariaDescribedBy=null;ariaLabelledBy=null;ariaLabel=null;ariaModal=!1;autoFocus="first-tabbable";restoreFocus=!0;delayFocusTrap=!0;scrollStrategy;closeOnNavigation=!0;enterAnimationDuration;exitAnimationDuration;bindings},vv="mdc-dialog--open",sE="mdc-dialog--opening",aE="mdc-dialog--closing",VP=150,BP=75,jP=(()=>{class t extends Sl{_animationStateChanged=new W;_animationsEnabled=!Ce();_actionSectionCount=0;_hostElement=this._elementRef.nativeElement;_enterAnimationDuration=this._animationsEnabled?cE(this._config.enterAnimationDuration)??VP:0;_exitAnimationDuration=this._animationsEnabled?cE(this._config.exitAnimationDuration)??BP:0;_animationTimer=null;_contentAttached(){super._contentAttached(),this._startOpenAnimation()}_startOpenAnimation(){this._animationStateChanged.emit({state:"opening",totalTime:this._enterAnimationDuration}),this._animationsEnabled?(this._hostElement.style.setProperty(lE,`${this._enterAnimationDuration}ms`),this._requestAnimationFrame(()=>this._hostElement.classList.add(sE,vv)),this._waitForAnimationToComplete(this._enterAnimationDuration,this._finishDialogOpen)):(this._hostElement.classList.add(vv),Promise.resolve().then(()=>this._finishDialogOpen()))}_startExitAnimation(){this._animationStateChanged.emit({state:"closing",totalTime:this._exitAnimationDuration}),this._hostElement.classList.remove(vv),this._animationsEnabled?(this._hostElement.style.setProperty(lE,`${this._exitAnimationDuration}ms`),this._requestAnimationFrame(()=>this._hostElement.classList.add(aE)),this._waitForAnimationToComplete(this._exitAnimationDuration,this._finishDialogClose)):Promise.resolve().then(()=>this._finishDialogClose())}_updateActionSectionCount(e){this._actionSectionCount+=e,this._changeDetectorRef.markForCheck()}_finishDialogOpen=()=>{this._clearAnimationClasses(),this._openAnimationDone(this._enterAnimationDuration)};_finishDialogClose=()=>{this._clearAnimationClasses(),this._animationStateChanged.emit({state:"closed",totalTime:this._exitAnimationDuration})};_clearAnimationClasses(){this._hostElement.classList.remove(sE,aE)}_waitForAnimationToComplete(e,i){this._animationTimer!==null&&clearTimeout(this._animationTimer),this._animationTimer=setTimeout(i,e)}_requestAnimationFrame(e){this._ngZone.runOutsideAngular(()=>{typeof requestAnimationFrame=="function"?requestAnimationFrame(e):e()})}_captureInitialFocus(){this._config.delayFocusTrap||this._trapFocus()}_openAnimationDone(e){this._config.delayFocusTrap&&this._trapFocus(),this._animationStateChanged.next({state:"opened",totalTime:e})}ngOnDestroy(){super.ngOnDestroy(),this._animationTimer!==null&&clearTimeout(this._animationTimer)}attachComponentPortal(e){let i=super.attachComponentPortal(e);return i.location.nativeElement.classList.add("mat-mdc-dialog-component-host"),i}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ht(t)))(r||t)}})();static \u0275cmp=T({type:t,selectors:[["mat-dialog-container"]],hostAttrs:["tabindex","-1",1,"mat-mdc-dialog-container","mdc-dialog"],hostVars:10,hostBindings:function(i,r){i&2&&(Et("id",r._config.id),re("aria-modal",r._config.ariaModal)("role",r._config.role)("aria-labelledby",r._config.ariaLabel?null:r._ariaLabelledByQueue[0])("aria-label",r._config.ariaLabel)("aria-describedby",r._config.ariaDescribedBy||null),z("_mat-animation-noopable",!r._animationsEnabled)("mat-mdc-dialog-container-with-actions",r._actionSectionCount>0))},features:[Me],decls:3,vars:0,consts:[[1,"mat-mdc-dialog-inner-container","mdc-dialog__container"],[1,"mat-mdc-dialog-surface","mdc-dialog__surface"],["cdkPortalOutlet",""]],template:function(i,r){i&1&&(m(0,"div",0)(1,"div",1),Dt(2,LP,0,0,"ng-template",2),g()())},dependencies:[Gn],styles:[`.mat-mdc-dialog-container {
  width: 100%;
  height: 100%;
  display: block;
  box-sizing: border-box;
  max-height: inherit;
  min-height: inherit;
  min-width: inherit;
  max-width: inherit;
  outline: 0;
}

.cdk-overlay-pane.mat-mdc-dialog-panel {
  max-width: var(--%NS%mat-dialog-container-max-width, 560px);
  min-width: var(--%NS%mat-dialog-container-min-width, 280px);
}
@media (max-width: 599px) {
  .cdk-overlay-pane.mat-mdc-dialog-panel {
    max-width: var(--%NS%mat-dialog-container-small-max-width, calc(100vw - 32px));
  }
}

.mat-mdc-dialog-inner-container {
  display: flex;
  flex-direction: row;
  align-items: center;
  justify-content: space-around;
  box-sizing: border-box;
  height: 100%;
  opacity: 0;
  transition: opacity linear var(--%NS%mat-dialog-transition-duration, 0ms);
  max-height: inherit;
  min-height: inherit;
  min-width: inherit;
  max-width: inherit;
}
.mdc-dialog--closing .mat-mdc-dialog-inner-container {
  transition: opacity 75ms linear;
  transform: none;
}
.mdc-dialog--open .mat-mdc-dialog-inner-container {
  opacity: 1;
}
._mat-animation-noopable .mat-mdc-dialog-inner-container {
  transition: none;
}

.mat-mdc-dialog-surface {
  display: flex;
  flex-direction: column;
  flex-grow: 0;
  flex-shrink: 0;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  position: relative;
  overflow-y: auto;
  outline: 0;
  transform: scale(0.8);
  transition: transform var(--%NS%mat-dialog-transition-duration, 0ms) cubic-bezier(0, 0, 0.2, 1);
  max-height: inherit;
  min-height: inherit;
  min-width: inherit;
  max-width: inherit;
  box-shadow: var(--%NS%mat-dialog-container-elevation-shadow, none);
  border-radius: var(--%NS%mat-dialog-container-shape, var(--%NS%mat-sys-corner-extra-large, 4px));
  background-color: var(--%NS%mat-dialog-container-color, var(--%NS%mat-sys-surface, white));
}
[dir=rtl] .mat-mdc-dialog-surface {
  text-align: right;
}
.mdc-dialog--open .mat-mdc-dialog-surface, .mdc-dialog--closing .mat-mdc-dialog-surface {
  transform: none;
}
._mat-animation-noopable .mat-mdc-dialog-surface {
  transition: none;
}
.mat-mdc-dialog-surface::before {
  position: absolute;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  border: 2px solid transparent;
  border-radius: inherit;
  content: "";
  pointer-events: none;
}

.mat-mdc-dialog-title {
  display: block;
  position: relative;
  flex-shrink: 0;
  box-sizing: border-box;
  margin: 0 0 1px;
  padding: var(--%NS%mat-dialog-headline-padding, 6px 24px 13px);
}
.mat-mdc-dialog-title::before {
  display: inline-block;
  width: 0;
  height: 40px;
  content: "";
  vertical-align: 0;
}
[dir=rtl] .mat-mdc-dialog-title {
  text-align: right;
}
.mat-mdc-dialog-container .mat-mdc-dialog-title {
  color: var(--%NS%mat-dialog-subhead-color, var(--%NS%mat-sys-on-surface, rgba(0, 0, 0, 0.87)));
  font-family: var(--%NS%mat-dialog-subhead-font, var(--%NS%mat-sys-headline-small-font, inherit));
  line-height: var(--%NS%mat-dialog-subhead-line-height, var(--%NS%mat-sys-headline-small-line-height, 1.5rem));
  font-size: var(--%NS%mat-dialog-subhead-size, var(--%NS%mat-sys-headline-small-size, 1rem));
  font-weight: var(--%NS%mat-dialog-subhead-weight, var(--%NS%mat-sys-headline-small-weight, 400));
  letter-spacing: var(--%NS%mat-dialog-subhead-tracking, var(--%NS%mat-sys-headline-small-tracking, 0.03125em));
}

.mat-mdc-dialog-content {
  display: block;
  flex-grow: 1;
  box-sizing: border-box;
  margin: 0;
  overflow: auto;
  max-height: 65vh;
}
.mat-mdc-dialog-content > :first-child {
  margin-top: 0;
}
.mat-mdc-dialog-content > :last-child {
  margin-bottom: 0;
}
.mat-mdc-dialog-container .mat-mdc-dialog-content {
  color: var(--%NS%mat-dialog-supporting-text-color, var(--%NS%mat-sys-on-surface-variant, rgba(0, 0, 0, 0.6)));
  font-family: var(--%NS%mat-dialog-supporting-text-font, var(--%NS%mat-sys-body-medium-font, inherit));
  line-height: var(--%NS%mat-dialog-supporting-text-line-height, var(--%NS%mat-sys-body-medium-line-height, 1.5rem));
  font-size: var(--%NS%mat-dialog-supporting-text-size, var(--%NS%mat-sys-body-medium-size, 1rem));
  font-weight: var(--%NS%mat-dialog-supporting-text-weight, var(--%NS%mat-sys-body-medium-weight, 400));
  letter-spacing: var(--%NS%mat-dialog-supporting-text-tracking, var(--%NS%mat-sys-body-medium-tracking, 0.03125em));
}
.mat-mdc-dialog-container .mat-mdc-dialog-content {
  padding: var(--%NS%mat-dialog-content-padding, 20px 24px);
}
.mat-mdc-dialog-container-with-actions .mat-mdc-dialog-content {
  padding: var(--%NS%mat-dialog-with-actions-content-padding, 20px 24px 0);
}
.mat-mdc-dialog-container .mat-mdc-dialog-title + .mat-mdc-dialog-content {
  padding-top: 0;
}

.mat-mdc-dialog-actions {
  display: flex;
  position: relative;
  flex-shrink: 0;
  flex-wrap: wrap;
  align-items: center;
  box-sizing: border-box;
  min-height: 52px;
  margin: 0;
  border-top: 1px solid transparent;
  padding: var(--%NS%mat-dialog-actions-padding, 16px 24px);
  justify-content: var(--%NS%mat-dialog-actions-alignment, flex-end);
}
@media (forced-colors: active) {
  .mat-mdc-dialog-actions {
    border-top-color: CanvasText;
  }
}
.mat-mdc-dialog-actions.mat-mdc-dialog-actions-align-start, .mat-mdc-dialog-actions[align=start] {
  justify-content: start;
}
.mat-mdc-dialog-actions.mat-mdc-dialog-actions-align-center, .mat-mdc-dialog-actions[align=center] {
  justify-content: center;
}
.mat-mdc-dialog-actions.mat-mdc-dialog-actions-align-end, .mat-mdc-dialog-actions[align=end] {
  justify-content: flex-end;
}
.mat-mdc-dialog-actions .mat-button-base + .mat-button-base,
.mat-mdc-dialog-actions .mat-mdc-button-base + .mat-mdc-button-base {
  margin-left: 8px;
}
[dir=rtl] .mat-mdc-dialog-actions .mat-button-base + .mat-button-base,
[dir=rtl] .mat-mdc-dialog-actions .mat-mdc-button-base + .mat-mdc-button-base {
  margin-left: 0;
  margin-right: 8px;
}

.mat-mdc-dialog-component-host {
  display: contents;
}
`],encapsulation:2,changeDetection:1})}return t})(),lE="--mat-dialog-transition-duration";function cE(t){return t==null?null:typeof t=="number"?t:t.endsWith("ms")?bi(t.substring(0,t.length-2)):t.endsWith("s")?bi(t.substring(0,t.length-1))*1e3:t==="0"?0:null}var rf=(function(t){return t[t.OPEN=0]="OPEN",t[t.CLOSING=1]="CLOSING",t[t.CLOSED=2]="CLOSED",t})(rf||{}),Ds=class{_ref;_config;_containerInstance;componentInstance;componentRef=null;disableClose;id;_afterOpened=new Ti(1);_beforeClosed=new Ti(1);_result;_closeFallbackTimeout;_state=rf.OPEN;_closeInteractionType;constructor(n,e,i){this._ref=n,this._config=e,this._containerInstance=i,this.disableClose=e.disableClose,this.id=n.id,n.addPanelClass("mat-mdc-dialog-panel"),i._animationStateChanged.pipe(ge(r=>r.state==="opened"),Ae(1)).subscribe(()=>{this._afterOpened.next(),this._afterOpened.complete()}),i._animationStateChanged.pipe(ge(r=>r.state==="closed"),Ae(1)).subscribe(()=>{clearTimeout(this._closeFallbackTimeout),this._finishDialogClose()}),n.overlayRef.detachments().subscribe(()=>{this._beforeClosed.next(this._result),this._beforeClosed.complete(),this._finishDialogClose()}),Nt(this.backdropClick(),this.keydownEvents().pipe(ge(r=>r.keyCode===27&&!this.disableClose&&!$e(r)))).subscribe(r=>{this.disableClose||(r.preventDefault(),HP(this,r.type==="keydown"?"keyboard":"mouse"))})}close(n){let e=this._config.closePredicate;e&&!e(n,this._config,this.componentInstance)||(this._result=n,this._containerInstance._animationStateChanged.pipe(ge(i=>i.state==="closing"),Ae(1)).subscribe(i=>{this._beforeClosed.next(n),this._beforeClosed.complete(),this._ref.overlayRef.detachBackdrop(),this._closeFallbackTimeout=setTimeout(()=>this._finishDialogClose(),i.totalTime+100)}),this._state=rf.CLOSING,this._containerInstance._startExitAnimation())}afterOpened(){return this._afterOpened}afterClosed(){return this._ref.closed}beforeClosed(){return this._beforeClosed}backdropClick(){return this._ref.backdropClick}keydownEvents(){return this._ref.keydownEvents}updatePosition(n){let e=this._ref.config.positionStrategy;return n&&(n.left||n.right)?n.left?e.left(n.left):e.right(n.right):e.centerHorizontally(),n&&(n.top||n.bottom)?n.top?e.top(n.top):e.bottom(n.bottom):e.centerVertically(),this._ref.updatePosition(),this}updateSize(n="",e=""){return this._ref.updateSize(n,e),this}addPanelClass(n){return this._ref.addPanelClass(n),this}removePanelClass(n){return this._ref.removePanelClass(n),this}getState(){return this._state}_finishDialogClose(){this._state=rf.CLOSED,this._ref.close(this._result,{focusOrigin:this._closeInteractionType}),this.componentInstance=null}};function HP(t,n,e){return t._closeInteractionType=n,t.close(e)}var UP=new b("MatMdcDialogData"),zP=new b("mat-mdc-dialog-default-options"),$P=new b("mat-mdc-dialog-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(R);return()=>rr(t)}}),Es=(()=>{class t{_defaultOptions=d(zP,{optional:!0});_scrollStrategy=d($P);_parentDialog=d(t,{optional:!0,skipSelf:!0});_idGenerator=d(xe);_injector=d(R);_dialog=d(Cl);_animationsDisabled=Ce();_openDialogsAtThisLevel=[];_afterAllClosedAtThisLevel=new E;_afterOpenedAtThisLevel=new E;dialogConfigClass=of;_dialogRefConstructor;_dialogContainerType;_dialogDataToken;get openDialogs(){return this._parentDialog?this._parentDialog.openDialogs:this._openDialogsAtThisLevel}get afterOpened(){return this._parentDialog?this._parentDialog.afterOpened:this._afterOpenedAtThisLevel}_getAfterAllClosed(){let e=this._parentDialog;return e?e._getAfterAllClosed():this._afterAllClosedAtThisLevel}afterAllClosed=cn(()=>this.openDialogs.length?this._getAfterAllClosed():this._getAfterAllClosed().pipe(nt(void 0)));constructor(){this._dialogRefConstructor=Ds,this._dialogContainerType=jP,this._dialogDataToken=UP}open(e,i){let r;i=v(v({},this._defaultOptions||new of),i),i.id=i.id||this._idGenerator.getId("mat-mdc-dialog-"),i.scrollStrategy=i.scrollStrategy||this._scrollStrategy();let o=this._dialog.open(e,k(v({},i),{positionStrategy:Wn(this._injector).centerHorizontally().centerVertically(),disableClose:!0,closePredicate:void 0,closeOnDestroy:!1,closeOnOverlayDetachments:!1,disableAnimations:this._animationsDisabled||i.enterAnimationDuration?.toLocaleString()==="0"||i.exitAnimationDuration?.toString()==="0",container:{type:this._dialogContainerType,providers:()=>[{provide:this.dialogConfigClass,useValue:i},{provide:or,useValue:i}]},templateContext:()=>({dialogRef:r}),providers:(s,a,l)=>(r=new this._dialogRefConstructor(s,i,l),r.updatePosition(i?.position),[{provide:this._dialogContainerType,useValue:l},{provide:this._dialogDataToken,useValue:a.data},{provide:this._dialogRefConstructor,useValue:r},{provide:ho,useValue:null}])}));return r.componentRef=o.componentRef,r.componentInstance=o.componentInstance,this.openDialogs.push(r),this.afterOpened.next(r),r.afterClosed().subscribe(()=>{let s=this.openDialogs.indexOf(r);s>-1&&(this.openDialogs.splice(s,1),this.openDialogs.length||this._getAfterAllClosed().next())}),r}closeAll(){this._closeDialogs(this.openDialogs)}getDialogById(e){return this.openDialogs.find(i=>i.id===e)}ngOnDestroy(){this._closeDialogs(this._openDialogsAtThisLevel),this._afterAllClosedAtThisLevel.complete(),this._afterOpenedAtThisLevel.complete()}_closeDialogs(e){let i=e.length;for(;i--;)e[i].close()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();var dE=(()=>{class t{_dialogRef=d(Ds,{optional:!0});_elementRef=d(O);_dialog=d(Es);ngOnInit(){this._dialogRef||(this._dialogRef=GP(this._elementRef,this._dialog.openDialogs)),this._dialogRef&&Promise.resolve().then(()=>{this._onAdd()})}ngOnDestroy(){this._dialogRef?._containerInstance&&Promise.resolve().then(()=>{this._onRemove()})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t})}return t})(),uE=(()=>{class t extends dE{id=d(xe).getId("mat-mdc-dialog-title-");_onAdd(){this._dialogRef._containerInstance?._addAriaLabelledBy?.(this.id)}_onRemove(){this._dialogRef?._containerInstance?._removeAriaLabelledBy?.(this.id)}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ht(t)))(r||t)}})();static \u0275dir=H({type:t,selectors:[["","mat-dialog-title",""],["","matDialogTitle",""]],hostAttrs:[1,"mat-mdc-dialog-title","mdc-dialog__title"],hostVars:1,hostBindings:function(i,r){i&2&&Et("id",r.id)},inputs:{id:"id"},exportAs:["matDialogTitle"],features:[Me]})}return t})(),fE=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,selectors:[["","mat-dialog-content",""],["mat-dialog-content"],["","matDialogContent",""]],hostAttrs:[1,"mat-mdc-dialog-content","mdc-dialog__content"],features:[Op([Qg])]})}return t})(),hE=(()=>{class t extends dE{align;_onAdd(){this._dialogRef._containerInstance?._updateActionSectionCount?.(1)}_onRemove(){this._dialogRef._containerInstance?._updateActionSectionCount?.(-1)}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ht(t)))(r||t)}})();static \u0275dir=H({type:t,selectors:[["","mat-dialog-actions",""],["mat-dialog-actions"],["","matDialogActions",""]],hostAttrs:[1,"mat-mdc-dialog-actions","mdc-dialog__actions"],hostVars:6,hostBindings:function(i,r){i&2&&z("mat-mdc-dialog-actions-align-start",r.align==="start")("mat-mdc-dialog-actions-align-center",r.align==="center")("mat-mdc-dialog-actions-align-end",r.align==="end")},inputs:{align:"align"},features:[Me]})}return t})();function GP(t,n){let e=t.nativeElement.parentElement;for(;e&&!e.classList.contains("mat-mdc-dialog-container");)e=e.parentElement;return e?n.find(i=>i.id===e.id):null}var mE=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({providers:[Es],imports:[ov,on,vi,ve]})}return t})();var sf=(()=>{class t{get vertical(){return this._vertical}set vertical(e){this._vertical=sn(e)}_vertical=!1;get inset(){return this._inset}set inset(e){this._inset=sn(e)}_inset=!1;static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-divider"]],hostAttrs:["role","separator",1,"mat-divider"],hostVars:7,hostBindings:function(i,r){i&2&&(re("aria-orientation",r.vertical?"vertical":"horizontal"),z("mat-divider-vertical",r.vertical)("mat-divider-horizontal",!r.vertical)("mat-divider-inset",r.inset))},inputs:{vertical:"vertical",inset:"inset"},decls:0,vars:0,template:function(i,r){},styles:[`.mat-divider {
  display: block;
  margin: 0;
  border-top-style: solid;
  border-top-color: var(--%NS%mat-divider-color, var(--%NS%mat-sys-outline-variant));
  border-top-width: var(--%NS%mat-divider-width, 1px);
}
.mat-divider.mat-divider-vertical {
  border-top: 0;
  border-right-style: solid;
  border-right-color: var(--%NS%mat-divider-color, var(--%NS%mat-sys-outline-variant));
  border-right-width: var(--%NS%mat-divider-width, 1px);
}
.mat-divider.mat-divider-inset {
  margin-left: 80px;
}
[dir=rtl] .mat-divider.mat-divider-inset {
  margin-left: auto;
  margin-right: 80px;
}
`],encapsulation:2})}return t})(),af=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({imports:[ve]})}return t})();var bv=class{_box;_destroyed=new E;_resizeSubject=new E;_resizeObserver;_elementObservables=new Map;constructor(n){this._box=n,typeof ResizeObserver<"u"&&(this._resizeObserver=new ResizeObserver(e=>this._resizeSubject.next(e)))}observe(n){return this._elementObservables.has(n)||this._elementObservables.set(n,new ne(e=>{let i=this._resizeSubject.subscribe(e);return this._resizeObserver?.observe(n,{box:this._box}),()=>{this._resizeObserver?.unobserve(n),i.unsubscribe(),this._elementObservables.delete(n)}}).pipe(ge(e=>e.some(i=>i.target===n)),Nc({bufferSize:1,refCount:!0}),ye(this._destroyed))),this._elementObservables.get(n)}destroy(){this._destroyed.next(),this._destroyed.complete(),this._resizeSubject.complete(),this._elementObservables.clear()}},gE=(()=>{class t{_cleanupErrorListener;_observers=new Map;_ngZone=d(N);constructor(){typeof ResizeObserver<"u"}ngOnDestroy(){for(let[,e]of this._observers)e.destroy();this._observers.clear(),this._cleanupErrorListener?.()}observe(e,i){let r=i?.box||"content-box";return this._observers.has(r)||this._observers.set(r,new bv(r)),this._observers.get(r).observe(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();var qP=["notch"],YP=["*"],vE=["iconPrefixContainer"],bE=["textPrefixContainer"],_E=["iconSuffixContainer"],yE=["textSuffixContainer"],ZP=["textField"],XP=["*",[["mat-label"]],[["","matPrefix",""],["","matIconPrefix",""]],[["","matTextPrefix",""]],[["","matTextSuffix",""]],[["","matSuffix",""],["","matIconSuffix",""]],[["mat-error"],["","matError",""]],[["mat-hint",3,"align","end"]],[["mat-hint","align","end"]]],KP=["*","mat-label","[matPrefix], [matIconPrefix]","[matTextPrefix]","[matTextSuffix]","[matSuffix], [matIconSuffix]","mat-error, [matError]","mat-hint:not([align='end'])","mat-hint[align='end']"];function QP(t,n){t&1&&ie(0,"span",21)}function JP(t,n){if(t&1&&(m(0,"label",20),he(1,1),V(2,QP,1,0,"span",21),g()),t&2){let e=w(2);U("floating",e._shouldLabelFloat())("monitorResize",e._hasOutline())("id",e._labelId),re("for",e._control.disableAutomaticLabeling?null:e._control.id),_(2),B(!e.hideRequiredMarker&&e._control.required?2:-1)}}function eF(t,n){if(t&1&&V(0,JP,3,5,"label",20),t&2){let e=w();B(e._hasFloatingLabel()?0:-1)}}function tF(t,n){t&1&&ie(0,"div",7)}function nF(t,n){}function iF(t,n){if(t&1&&Dt(0,nF,0,0,"ng-template",13),t&2){w(2);let e=ht(1);U("ngTemplateOutlet",e)}}function rF(t,n){if(t&1&&(m(0,"div",9),V(1,iF,1,1,null,13),g()),t&2){let e=w();U("matFormFieldNotchedOutlineOpen",e._shouldLabelFloat()),_(),B(e._forceDisplayInfixLabel()?-1:1)}}function oF(t,n){t&1&&(m(0,"div",10,2),he(2,2),g())}function sF(t,n){t&1&&(m(0,"div",11,3),he(2,3),g())}function aF(t,n){}function lF(t,n){if(t&1&&Dt(0,aF,0,0,"ng-template",13),t&2){w();let e=ht(1);U("ngTemplateOutlet",e)}}function cF(t,n){t&1&&(m(0,"div",14,4),he(2,4),g())}function dF(t,n){t&1&&(m(0,"div",15,5),he(2,5),g())}function uF(t,n){t&1&&ie(0,"div",16)}function fF(t,n){t&1&&(m(0,"div",18),he(1,6),g())}function hF(t,n){if(t&1&&(m(0,"mat-hint",22),y(1),g()),t&2){let e=w(2);U("id",e._hintLabelId),_(),Te(e.hintLabel)}}function mF(t,n){if(t&1&&(m(0,"div",19),V(1,hF,2,2,"mat-hint",22),he(2,7),ie(3,"div",23),he(4,8),g()),t&2){let e=w();_(),B(e.hintLabel?1:-1)}}var Di=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,selectors:[["mat-label"]]})}return t})(),IE=new b("MatError"),yv=(()=>{class t{id=d(xe).getId("mat-mdc-error-");static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,selectors:[["mat-error"],["","matError",""]],hostAttrs:[1,"mat-mdc-form-field-error","mat-mdc-form-field-bottom-align"],hostVars:1,hostBindings:function(i,r){i&2&&Et("id",r.id)},inputs:{id:"id"},features:[Be([{provide:IE,useExisting:t}])]})}return t})(),_v=(()=>{class t{align="start";id=d(xe).getId("mat-mdc-hint-");static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,selectors:[["mat-hint"]],hostAttrs:[1,"mat-mdc-form-field-hint","mat-mdc-form-field-bottom-align"],hostVars:4,hostBindings:function(i,r){i&2&&(Et("id",r.id),re("align",null),z("mat-mdc-form-field-hint-end",r.align==="end"))},inputs:{align:"align",id:"id"}})}return t})(),pF=new b("MatPrefix");var gF=new b("MatSuffix");var ME=new b("FloatingLabelParent"),SE=(()=>{class t{_elementRef=d(O);get floating(){return this._floating}set floating(e){this._floating=e,this.monitorResize&&this._handleResize()}_floating=!1;get monitorResize(){return this._monitorResize}set monitorResize(e){this._monitorResize=e,this._monitorResize?this._subscribeToResize():this._resizeSubscription.unsubscribe()}_monitorResize=!1;_resizeObserver=d(gE);_ngZone=d(N);_parent=d(ME);_resizeSubscription=new pe;ngOnDestroy(){this._resizeSubscription.unsubscribe()}getWidth(){return vF(this._elementRef.nativeElement)}get element(){return this._elementRef.nativeElement}_handleResize(){setTimeout(()=>this._parent._handleLabelResized())}_subscribeToResize(){this._resizeSubscription.unsubscribe(),this._ngZone.runOutsideAngular(()=>{this._resizeSubscription=this._resizeObserver.observe(this._elementRef.nativeElement,{box:"border-box"}).subscribe(()=>this._handleResize())})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,selectors:[["label","matFormFieldFloatingLabel",""]],hostAttrs:[1,"mdc-floating-label","mat-mdc-floating-label"],hostVars:2,hostBindings:function(i,r){i&2&&z("mdc-floating-label--float-above",r.floating)},inputs:{floating:"floating",monitorResize:"monitorResize"}})}return t})();function vF(t){let n=t;if(n.offsetParent!==null)return n.scrollWidth;let e=n.cloneNode(!0);e.style.setProperty("position","absolute"),e.style.setProperty("transform","translate(-9999px, -9999px)"),document.documentElement.appendChild(e);let i=e.scrollWidth;return e.remove(),i}var CE="mdc-line-ripple--active",lf="mdc-line-ripple--deactivating",DE=(()=>{class t{_elementRef=d(O);_cleanupTransitionEnd;constructor(){let e=d(N),i=d(Re);e.runOutsideAngular(()=>{this._cleanupTransitionEnd=i.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionEnd)})}activate(){let e=this._elementRef.nativeElement.classList;e.remove(lf),e.add(CE)}deactivate(){this._elementRef.nativeElement.classList.add(lf)}_handleTransitionEnd=e=>{let i=this._elementRef.nativeElement.classList,r=i.contains(lf);e.propertyName==="opacity"&&r&&i.remove(CE,lf)};ngOnDestroy(){this._cleanupTransitionEnd()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,selectors:[["div","matFormFieldLineRipple",""]],hostAttrs:[1,"mdc-line-ripple"]})}return t})(),EE=(()=>{class t{_elementRef=d(O);_ngZone=d(N);open=!1;_notch;ngAfterViewInit(){let e=this._elementRef.nativeElement,i=e.querySelector(".mdc-floating-label");i?(e.classList.add("mdc-notched-outline--upgraded"),typeof requestAnimationFrame=="function"&&(i.style.transitionDuration="0s",this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>i.style.transitionDuration="")}))):e.classList.add("mdc-notched-outline--no-label")}_setNotchWidth(e){let i=this._notch.nativeElement;!this.open||!e?i.style.width="":i.style.width=`calc(${e}px * var(--mat-mdc-form-field-floating-label-scale, 0.75) + 9px)`}_setMaxWidth(e){this._notch.nativeElement.style.setProperty("--mat-form-field-notch-max-width",`calc(100% - ${e}px)`)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["div","matFormFieldNotchedOutline",""]],viewQuery:function(i,r){if(i&1&&Ue(qP,5),i&2){let o;ce(o=de())&&(r._notch=o.first)}},hostAttrs:[1,"mdc-notched-outline"],hostVars:2,hostBindings:function(i,r){i&2&&z("mdc-notched-outline--notched",r.open)},inputs:{open:[0,"matFormFieldNotchedOutlineOpen","open"]},ngContentSelectors:YP,decls:5,vars:0,consts:[["notch",""],[1,"mat-mdc-notch-piece","mdc-notched-outline__leading"],[1,"mat-mdc-notch-piece","mdc-notched-outline__notch"],[1,"mat-mdc-notch-piece","mdc-notched-outline__trailing"]],template:function(i,r){i&1&&(Pe(),Qt(0,"div",1),lt(1,"div",2,0),he(3),_t(),Qt(4,"div",3))},encapsulation:2})}return t})(),Ml=(()=>{class t{value=null;stateChanges;id;placeholder;ngControl=null;focused=!1;empty=!1;shouldLabelFloat=!1;required=!1;disabled=!1;errorState=!1;controlType;autofilled;userAriaDescribedBy;disableAutomaticLabeling;describedByIds;static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t})}return t})();var Tl=new b("MatFormField"),bF=new b("MAT_FORM_FIELD_DEFAULT_OPTIONS"),wE="fill",_F="auto",xE="fixed",yF="translateY(-50%)",cr=(()=>{class t{_elementRef=d(O);_changeDetectorRef=d(Oe);_platform=d(be);_idGenerator=d(xe);_ngZone=d(N);_defaults=d(bF,{optional:!0});_currentDirection;_textField;_iconPrefixContainer;_textPrefixContainer;_iconSuffixContainer;_textSuffixContainer;_floatingLabel;_notchedOutline;_lineRipple;_iconPrefixContainerSignal=ui("iconPrefixContainer");_textPrefixContainerSignal=ui("textPrefixContainer");_iconSuffixContainerSignal=ui("iconSuffixContainer");_textSuffixContainerSignal=ui("textSuffixContainer");_prefixSuffixContainers=Q(()=>[this._iconPrefixContainerSignal(),this._textPrefixContainerSignal(),this._iconSuffixContainerSignal(),this._textSuffixContainerSignal()].map(e=>e?.nativeElement).filter(e=>e!==void 0));_formFieldControl;_prefixChildren;_suffixChildren;_errorChildren;_hintChildren;_labelChild=y0(Di);get hideRequiredMarker(){return this._hideRequiredMarker}set hideRequiredMarker(e){this._hideRequiredMarker=sn(e)}_hideRequiredMarker=!1;color="primary";get floatLabel(){return this._floatLabel||this._defaults?.floatLabel||_F}set floatLabel(e){e!==this._floatLabel&&(this._floatLabel=e,this._changeDetectorRef.markForCheck())}_floatLabel;get appearance(){return this._appearanceSignal()}set appearance(e){let i=e||this._defaults?.appearance||wE;this._appearanceSignal.set(i)}_appearanceSignal=j(wE);get subscriptSizing(){return this._subscriptSizing||this._defaults?.subscriptSizing||xE}set subscriptSizing(e){this._subscriptSizing=e||this._defaults?.subscriptSizing||xE}_subscriptSizing=null;get hintLabel(){return this._hintLabel}set hintLabel(e){this._hintLabel=e,this._processHints()}_hintLabel="";_hasIconPrefix=!1;_hasTextPrefix=!1;_hasIconSuffix=!1;_hasTextSuffix=!1;_labelId=this._idGenerator.getId("mat-mdc-form-field-label-");_hintLabelId=this._idGenerator.getId("mat-mdc-hint-");_describedByIds;get _control(){return this._explicitFormFieldControl||this._formFieldControl}set _control(e){this._explicitFormFieldControl=e}_destroyed=new E;_isFocused=null;_explicitFormFieldControl;_previousControl=null;_previousControlValidatorFn=null;_stateChanges;_valueChanges;_describedByChanges;_outlineLabelOffsetResizeObserver=null;_animationsDisabled=Ce();constructor(){let e=this._defaults,i=d(ct);e&&(e.appearance&&(this.appearance=e.appearance),this._hideRequiredMarker=!!e?.hideRequiredMarker,e.color&&(this.color=e.color)),ut(()=>this._currentDirection=i.valueSignal()),this._syncOutlineLabelOffset()}ngAfterViewInit(){this._updateFocusState(),this._animationsDisabled||this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-form-field-animations-enabled")},300)}),this._changeDetectorRef.detectChanges()}ngAfterContentInit(){this._assertFormFieldControl(),this._initializeSubscript(),this._initializePrefixAndSuffix()}ngAfterContentChecked(){this._assertFormFieldControl(),this._control!==this._previousControl&&(this._initializeControl(this._previousControl),this._control.ngControl&&this._control.ngControl.control&&(this._previousControlValidatorFn=this._control.ngControl.control.validator),this._previousControl=this._control,this._changeDetectorRef.markForCheck()),this._control.ngControl&&this._control.ngControl.control&&this._control.ngControl.control.validator!==this._previousControlValidatorFn&&this._changeDetectorRef.markForCheck()}ngOnDestroy(){this._outlineLabelOffsetResizeObserver?.disconnect(),this._stateChanges?.unsubscribe(),this._valueChanges?.unsubscribe(),this._describedByChanges?.unsubscribe(),this._destroyed.next(),this._destroyed.complete()}getLabelId=Q(()=>this._hasFloatingLabel()?this._labelId:null);getConnectedOverlayOrigin(){return this._textField||this._elementRef}_animateAndLockLabel(){this._hasFloatingLabel()&&(this.floatLabel="always")}_initializeControl(e){let i=this._control,r="mat-mdc-form-field-type-";e&&this._elementRef.nativeElement.classList.remove(r+e.controlType),i.controlType&&this._elementRef.nativeElement.classList.add(r+i.controlType),this._stateChanges?.unsubscribe(),this._stateChanges=i.stateChanges.subscribe(()=>{this._updateFocusState(),this._changeDetectorRef.markForCheck()}),this._describedByChanges?.unsubscribe(),this._describedByChanges=i.stateChanges.pipe(nt([void 0,void 0]),oe(()=>[i.errorState,i.userAriaDescribedBy]),Tc(),ge(([[o,s],[a,l]])=>o!==a||s!==l)).subscribe(()=>this._syncDescribedByIds()),this._valueChanges?.unsubscribe(),i.ngControl&&i.ngControl.valueChanges&&(this._valueChanges=i.ngControl.valueChanges.pipe(ye(this._destroyed)).subscribe(()=>this._changeDetectorRef.markForCheck()))}_checkPrefixAndSuffixTypes(){this._hasIconPrefix=!!this._prefixChildren.find(e=>!e._isText),this._hasTextPrefix=!!this._prefixChildren.find(e=>e._isText),this._hasIconSuffix=!!this._suffixChildren.find(e=>!e._isText),this._hasTextSuffix=!!this._suffixChildren.find(e=>e._isText)}_initializePrefixAndSuffix(){this._checkPrefixAndSuffixTypes(),Nt(this._prefixChildren.changes,this._suffixChildren.changes).subscribe(()=>{this._checkPrefixAndSuffixTypes(),this._changeDetectorRef.markForCheck()})}_initializeSubscript(){this._hintChildren.changes.subscribe(()=>{this._processHints(),this._changeDetectorRef.markForCheck()}),this._errorChildren.changes.subscribe(()=>{this._syncDescribedByIds(),this._changeDetectorRef.markForCheck()}),this._validateHints(),this._syncDescribedByIds()}_assertFormFieldControl(){this._control}_updateFocusState(){let e=this._control.focused;e&&!this._isFocused?(this._isFocused=!0,this._lineRipple?.activate()):!e&&(this._isFocused||this._isFocused===null)&&(this._isFocused=!1,this._lineRipple?.deactivate()),this._elementRef.nativeElement.classList.toggle("mat-focused",e),this._textField?.nativeElement.classList.toggle("mdc-text-field--focused",e)}_syncOutlineLabelOffset(){Zr({earlyRead:()=>{if(this._appearanceSignal()!=="outline")return this._outlineLabelOffsetResizeObserver?.disconnect(),null;if(globalThis.ResizeObserver){this._outlineLabelOffsetResizeObserver||=new globalThis.ResizeObserver(()=>{this._writeOutlinedLabelStyles(this._getOutlinedLabelOffset())});for(let e of this._prefixSuffixContainers())this._outlineLabelOffsetResizeObserver.observe(e,{box:"border-box"})}return this._getOutlinedLabelOffset()},write:e=>this._writeOutlinedLabelStyles(e())})}_shouldAlwaysFloat(){return this.floatLabel==="always"}_hasOutline(){return this.appearance==="outline"}_forceDisplayInfixLabel(){return!this._platform.isBrowser&&this._prefixChildren.length&&!this._shouldLabelFloat()}_hasFloatingLabel=Q(()=>!!this._labelChild());_shouldLabelFloat(){return this._hasFloatingLabel()?this._control.shouldLabelFloat||this._shouldAlwaysFloat():!1}_shouldForward(e){let i=this._control?this._control.ngControl:null;return i&&i[e]}_getSubscriptMessageType(){return this._errorChildren&&this._errorChildren.length>0&&this._control.errorState?"error":"hint"}_handleLabelResized(){this._refreshOutlineNotchWidth()}_refreshOutlineNotchWidth(){!this._hasOutline()||!this._floatingLabel||!this._shouldLabelFloat()?this._notchedOutline?._setNotchWidth(0):this._notchedOutline?._setNotchWidth(this._floatingLabel.getWidth())}_processHints(){this._validateHints(),this._syncDescribedByIds()}_validateHints(){this._hintChildren}_syncDescribedByIds(){if(this._control){let e=[];if(this._control.userAriaDescribedBy&&typeof this._control.userAriaDescribedBy=="string"&&e.push(...this._control.userAriaDescribedBy.split(" ")),this._getSubscriptMessageType()==="hint"){let o=this._hintChildren?this._hintChildren.find(a=>a.align==="start"):null,s=this._hintChildren?this._hintChildren.find(a=>a.align==="end"):null;o?e.push(o.id):this._hintLabel&&e.push(this._hintLabelId),s&&e.push(s.id)}else this._errorChildren&&e.push(...this._errorChildren.map(o=>o.id));let i=this._control.describedByIds,r;if(i){let o=this._describedByIds||e;r=e.concat(i.filter(s=>s&&!o.includes(s)))}else r=e;this._control.setDescribedByIds(r),this._describedByIds=e}}_getOutlinedLabelOffset(){if(!this._hasOutline()||!this._floatingLabel)return null;if(!this._iconPrefixContainer&&!this._textPrefixContainer)return["",null];if(!this._isAttachedToDom())return null;let e=this._iconPrefixContainer?.nativeElement,i=this._textPrefixContainer?.nativeElement,r=this._iconSuffixContainer?.nativeElement,o=this._textSuffixContainer?.nativeElement,s=e?.getBoundingClientRect().width??0,a=i?.getBoundingClientRect().width??0,l=r?.getBoundingClientRect().width??0,c=o?.getBoundingClientRect().width??0,u=this._currentDirection==="rtl"?"-1":"1",f=`${s+a}px`,p=`calc(${u} * (${f} + var(--mat-mdc-form-field-label-offset-x, 0px)))`,S=`var(--mat-mdc-form-field-label-transform, ${yF} translateX(${p}))`,C=s+a+l+c;return[S,C]}_writeOutlinedLabelStyles(e){if(e!==null){let[i,r]=e;this._floatingLabel&&(this._floatingLabel.element.style.transform=i),r!==null&&this._notchedOutline?._setMaxWidth(r)}}_isAttachedToDom(){let e=this._elementRef.nativeElement;if(e.getRootNode){let i=e.getRootNode();return i&&i!==e}return document.documentElement.contains(e)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-form-field"]],contentQueries:function(i,r,o){if(i&1&&(Wd(o,r._labelChild,Di,5),zn(o,Ml,5)(o,pF,5)(o,gF,5)(o,IE,5)(o,_v,5)),i&2){Gi();let s;ce(s=de())&&(r._formFieldControl=s.first),ce(s=de())&&(r._prefixChildren=s),ce(s=de())&&(r._suffixChildren=s),ce(s=de())&&(r._errorChildren=s),ce(s=de())&&(r._hintChildren=s)}},viewQuery:function(i,r){if(i&1&&($i(r._iconPrefixContainerSignal,vE,5)(r._textPrefixContainerSignal,bE,5)(r._iconSuffixContainerSignal,_E,5)(r._textSuffixContainerSignal,yE,5),Ue(ZP,5)(vE,5)(bE,5)(_E,5)(yE,5)(SE,5)(EE,5)(DE,5)),i&2){Gi(4);let o;ce(o=de())&&(r._textField=o.first),ce(o=de())&&(r._iconPrefixContainer=o.first),ce(o=de())&&(r._textPrefixContainer=o.first),ce(o=de())&&(r._iconSuffixContainer=o.first),ce(o=de())&&(r._textSuffixContainer=o.first),ce(o=de())&&(r._floatingLabel=o.first),ce(o=de())&&(r._notchedOutline=o.first),ce(o=de())&&(r._lineRipple=o.first)}},hostAttrs:[1,"mat-mdc-form-field"],hostVars:38,hostBindings:function(i,r){i&2&&z("mat-mdc-form-field-label-always-float",r._shouldAlwaysFloat())("mat-mdc-form-field-has-icon-prefix",r._hasIconPrefix)("mat-mdc-form-field-has-icon-suffix",r._hasIconSuffix)("mat-form-field-invalid",r._control.errorState)("mat-form-field-disabled",r._control.disabled)("mat-form-field-autofilled",r._control.autofilled)("mat-form-field-appearance-fill",r.appearance=="fill")("mat-form-field-appearance-outline",r.appearance=="outline")("mat-form-field-hide-placeholder",r._hasFloatingLabel()&&!r._shouldLabelFloat())("mat-primary",r.color!=="accent"&&r.color!=="warn")("mat-accent",r.color==="accent")("mat-warn",r.color==="warn")("ng-untouched",r._shouldForward("untouched"))("ng-touched",r._shouldForward("touched"))("ng-pristine",r._shouldForward("pristine"))("ng-dirty",r._shouldForward("dirty"))("ng-valid",r._shouldForward("valid"))("ng-invalid",r._shouldForward("invalid"))("ng-pending",r._shouldForward("pending"))},inputs:{hideRequiredMarker:"hideRequiredMarker",color:"color",floatLabel:"floatLabel",appearance:"appearance",subscriptSizing:"subscriptSizing",hintLabel:"hintLabel"},exportAs:["matFormField"],features:[Be([{provide:Tl,useExisting:t},{provide:ME,useExisting:t}])],ngContentSelectors:KP,decls:18,vars:21,consts:[["labelTemplate",""],["textField",""],["iconPrefixContainer",""],["textPrefixContainer",""],["textSuffixContainer",""],["iconSuffixContainer",""],[1,"mat-mdc-text-field-wrapper","mdc-text-field",3,"click"],[1,"mat-mdc-form-field-focus-overlay"],[1,"mat-mdc-form-field-flex"],["matFormFieldNotchedOutline","",3,"matFormFieldNotchedOutlineOpen"],[1,"mat-mdc-form-field-icon-prefix"],[1,"mat-mdc-form-field-text-prefix"],[1,"mat-mdc-form-field-infix"],[3,"ngTemplateOutlet"],[1,"mat-mdc-form-field-text-suffix"],[1,"mat-mdc-form-field-icon-suffix"],["matFormFieldLineRipple",""],["aria-atomic","true","aria-live","polite",1,"mat-mdc-form-field-subscript-wrapper","mat-mdc-form-field-bottom-align"],[1,"mat-mdc-form-field-error-wrapper"],[1,"mat-mdc-form-field-hint-wrapper"],["matFormFieldFloatingLabel","",3,"floating","monitorResize","id"],["aria-hidden","true",1,"mat-mdc-form-field-required-marker","mdc-floating-label--required"],[3,"id"],[1,"mat-mdc-form-field-hint-spacer"]],template:function(i,r){if(i&1&&(Pe(XP),Dt(0,eF,1,1,"ng-template",null,0,Vp),m(2,"div",6,1),D("click",function(s){return r._control.onContainerClick(s)}),V(4,tF,1,0,"div",7),m(5,"div",8),V(6,rF,2,2,"div",9),V(7,oF,3,0,"div",10),V(8,sF,3,0,"div",11),m(9,"div",12),V(10,lF,1,1,null,13),he(11),g(),V(12,cF,3,0,"div",14),V(13,dF,3,0,"div",15),g(),V(14,uF,1,0,"div",16),g(),m(15,"div",17),V(16,fF,2,0,"div",18)(17,mF,5,1,"div",19),g()),i&2){let o;_(2),z("mdc-text-field--filled",!r._hasOutline())("mdc-text-field--outlined",r._hasOutline())("mdc-text-field--no-label",!r._hasFloatingLabel())("mdc-text-field--disabled",r._control.disabled)("mdc-text-field--invalid",r._control.errorState),_(2),B(!r._hasOutline()&&!r._control.disabled?4:-1),_(2),B(r._hasOutline()?6:-1),_(),B(r._hasIconPrefix?7:-1),_(),B(r._hasTextPrefix?8:-1),_(2),B(!r._hasOutline()||r._forceDisplayInfixLabel()?10:-1),_(2),B(r._hasTextSuffix?12:-1),_(),B(r._hasIconSuffix?13:-1),_(),B(r._hasOutline()?-1:14),_(),z("mat-mdc-form-field-subscript-dynamic-size",r.subscriptSizing==="dynamic");let s=r._getSubscriptMessageType();_(),B((o=s)==="error"?16:o==="hint"?17:-1)}},dependencies:[SE,EE,Gp,DE,_v],styles:[`.mdc-text-field {
  display: inline-flex;
  align-items: baseline;
  padding: 0 16px;
  position: relative;
  box-sizing: border-box;
  overflow: hidden;
  will-change: opacity, transform, color;
  border-top-left-radius: 4px;
  border-top-right-radius: 4px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
}

.mdc-text-field__input {
  width: 100%;
  min-width: 0;
  border: none;
  border-radius: 0;
  background: none;
  padding: 0;
  -moz-appearance: none;
  -webkit-appearance: none;
  height: 28px;
}
.mdc-text-field__input::-webkit-calendar-picker-indicator, .mdc-text-field__input::-webkit-search-cancel-button {
  display: none;
}
.mdc-text-field__input::-ms-clear {
  display: none;
}
.mdc-text-field__input:focus {
  outline: none;
}
.mdc-text-field__input:invalid {
  box-shadow: none;
}
.mdc-text-field__input::placeholder {
  opacity: 0;
}
.mdc-text-field__input::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field__input::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field__input:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mdc-text-field--focused .mdc-text-field__input::placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  opacity: 1;
}
.mdc-text-field--%NS%disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::placeholder {
  opacity: 0;
}
.mdc-text-field--%NS%disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field--%NS%disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field--%NS%disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--outlined .mdc-text-field__input, .mdc-text-field--filled.mdc-text-field--no-label .mdc-text-field__input {
  height: 100%;
}
.mdc-text-field--outlined .mdc-text-field__input {
  display: flex;
  border: none !important;
  background-color: transparent;
}
.mdc-text-field--disabled .mdc-text-field__input {
  pointer-events: auto;
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--%NS%mat-form-field-filled-input-text-color, var(--%NS%mat-sys-on-surface));
  caret-color: var(--%NS%mat-form-field-filled-caret-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--%NS%mat-form-field-filled-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--%NS%mat-form-field-filled-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--%NS%mat-form-field-filled-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--%NS%mat-form-field-filled-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--%NS%mat-form-field-outlined-input-text-color, var(--%NS%mat-sys-on-surface));
  caret-color: var(--%NS%mat-form-field-outlined-caret-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--%NS%mat-form-field-outlined-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--%NS%mat-form-field-outlined-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--%NS%mat-form-field-outlined-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--%NS%mat-form-field-outlined-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--%NS%mat-form-field-filled-error-caret-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--outlined.mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--%NS%mat-form-field-outlined-error-caret-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--%NS%mat-form-field-filled-disabled-input-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--%NS%mat-form-field-outlined-disabled-input-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-text-field__input {
    background-color: Window;
  }
}

.mdc-text-field--filled {
  height: 56px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
  border-top-left-radius: var(--%NS%mat-form-field-filled-container-shape, var(--%NS%mat-sys-corner-extra-small));
  border-top-right-radius: var(--%NS%mat-form-field-filled-container-shape, var(--%NS%mat-sys-corner-extra-small));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) {
  background-color: var(--%NS%mat-form-field-filled-container-color, var(--%NS%mat-sys-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled {
  background-color: var(--%NS%mat-form-field-filled-disabled-container-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 4%, transparent));
}

.mdc-text-field--outlined {
  height: 56px;
  overflow: visible;
  padding-right: max(16px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)));
  padding-left: max(16px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)) + 4px);
}
[dir=rtl] .mdc-text-field--outlined {
  padding-right: max(16px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)) + 4px);
  padding-left: max(16px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)));
}

.mdc-floating-label {
  position: absolute;
  left: 0;
  transform-origin: left top;
  line-height: 1.15rem;
  text-align: left;
  text-overflow: ellipsis;
  white-space: nowrap;
  cursor: text;
  overflow: hidden;
  will-change: transform;
}
[dir=rtl] .mdc-floating-label {
  right: 0;
  left: auto;
  transform-origin: right top;
  text-align: right;
}
.mdc-text-field .mdc-floating-label {
  top: 50%;
  transform: translateY(-50%);
  pointer-events: none;
}
.mdc-notched-outline .mdc-floating-label {
  display: inline-block;
  position: relative;
  max-width: 100%;
}
.mdc-text-field--outlined .mdc-floating-label {
  left: 4px;
  right: auto;
}
[dir=rtl] .mdc-text-field--outlined .mdc-floating-label {
  left: auto;
  right: 4px;
}
.mdc-text-field--filled .mdc-floating-label {
  left: 16px;
  right: auto;
}
[dir=rtl] .mdc-text-field--filled .mdc-floating-label {
  left: auto;
  right: 16px;
}
.mdc-text-field--disabled .mdc-floating-label {
  cursor: default;
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-floating-label {
    z-index: 1;
  }
}
.mdc-text-field--filled.mdc-text-field--no-label .mdc-floating-label {
  display: none;
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-label-text-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-focus-label-text-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-hover-label-text-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-error-label-text-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-error-focus-label-text-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-error-hover-label-text-color, var(--%NS%mat-sys-on-error-container));
}
.mdc-text-field--filled .mdc-floating-label {
  font-family: var(--%NS%mat-form-field-filled-label-text-font, var(--%NS%mat-sys-body-large-font));
  font-size: var(--%NS%mat-form-field-filled-label-text-size, var(--%NS%mat-sys-body-large-size));
  font-weight: var(--%NS%mat-form-field-filled-label-text-weight, var(--%NS%mat-sys-body-large-weight));
  letter-spacing: var(--%NS%mat-form-field-filled-label-text-tracking, var(--%NS%mat-sys-body-large-tracking));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-label-text-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-focus-label-text-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-hover-label-text-color, var(--%NS%mat-sys-on-surface));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-error-label-text-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-error-focus-label-text-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-error-hover-label-text-color, var(--%NS%mat-sys-on-error-container));
}
.mdc-text-field--outlined .mdc-floating-label {
  font-family: var(--%NS%mat-form-field-outlined-label-text-font, var(--%NS%mat-sys-body-large-font));
  font-size: var(--%NS%mat-form-field-outlined-label-text-size, var(--%NS%mat-sys-body-large-size));
  font-weight: var(--%NS%mat-form-field-outlined-label-text-weight, var(--%NS%mat-sys-body-large-weight));
  letter-spacing: var(--%NS%mat-form-field-outlined-label-text-tracking, var(--%NS%mat-sys-body-large-tracking));
}

.mdc-floating-label--float-above {
  cursor: auto;
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--filled .mdc-floating-label--float-above {
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--outlined .mdc-floating-label--float-above {
  transform: translateY(-37.25px) scale(1);
  font-size: 0.75rem;
}
.mdc-notched-outline .mdc-floating-label--float-above {
  text-overflow: clip;
}
.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: 133.3333333333%;
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  transform: translateY(-34.75px) scale(0.75);
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: 1rem;
}

.mdc-floating-label--%NS%required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 1px;
  margin-right: 0;
  content: "*";
}
[dir=rtl] .mdc-floating-label--%NS%required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 0;
  margin-right: 1px;
}

.mdc-notched-outline {
  display: flex;
  position: absolute;
  top: 0;
  right: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  max-width: 100%;
  height: 100%;
  text-align: left;
  pointer-events: none;
}
[dir=rtl] .mdc-notched-outline {
  text-align: right;
}
.mdc-text-field--outlined .mdc-notched-outline {
  z-index: 1;
}

.mat-mdc-notch-piece {
  box-sizing: border-box;
  height: 100%;
  pointer-events: none;
  border: none;
  border-top: 1px solid;
  border-bottom: 1px solid;
}
.mdc-text-field--focused .mat-mdc-notch-piece {
  border-width: 2px;
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-outline-color, var(--%NS%mat-sys-outline));
  border-width: var(--%NS%mat-form-field-outlined-outline-width, 1px);
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-hover-outline-color, var(--%NS%mat-sys-on-surface));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-focus-outline-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-disabled-outline-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-error-outline-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--%NS%invalid:not(.mdc-text-field--focused):hover .mdc-notched-outline .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-error-hover-outline-color, var(--%NS%mat-sys-on-error-container));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-error-focus-outline-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-notched-outline .mat-mdc-notch-piece {
  border-width: var(--%NS%mat-form-field-outlined-focus-outline-width, 2px);
}

.mdc-notched-outline__leading {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__leading {
  width: max(12px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)));
}
[dir=rtl] .mdc-notched-outline__leading {
  border-left: none;
  border-right: 1px solid;
  border-bottom-left-radius: 0;
  border-top-left-radius: 0;
  border-top-right-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
}

.mdc-notched-outline__trailing {
  flex-grow: 1;
  border-left: none;
  border-right: 1px solid;
  border-top-left-radius: 0;
  border-bottom-left-radius: 0;
  border-top-right-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
}
[dir=rtl] .mdc-notched-outline__trailing {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
}

.mdc-notched-outline__notch {
  flex: 0 0 auto;
  width: auto;
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__notch {
  max-width: min(var(--%NS%mat-form-field-notch-max-width, 100%), calc(100% - max(12px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  max-width: min(100%, calc(100% - max(12px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 1px;
}
.mdc-text-field--focused.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 2px;
}
.mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 0;
  padding-right: 8px;
  border-top: none;
}
[dir=rtl] .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 8px;
  padding-right: 0;
}
.mdc-notched-outline--no-label .mdc-notched-outline__notch {
  display: none;
}

.mdc-line-ripple::before, .mdc-line-ripple::after {
  position: absolute;
  bottom: 0;
  left: 0;
  width: 100%;
  border-bottom-style: solid;
  content: "";
}
.mdc-line-ripple::before {
  z-index: 1;
  border-bottom-width: var(--%NS%mat-form-field-filled-active-indicator-height, 1px);
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-line-ripple::before {
  border-bottom-color: var(--%NS%mat-form-field-filled-active-indicator-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--%NS%mat-form-field-filled-hover-active-indicator-color, var(--%NS%mat-sys-on-surface));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-line-ripple::before {
  border-bottom-color: var(--%NS%mat-form-field-filled-disabled-active-indicator-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-line-ripple::before {
  border-bottom-color: var(--%NS%mat-form-field-filled-error-active-indicator-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--%NS%invalid:not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--%NS%mat-form-field-filled-error-hover-active-indicator-color, var(--%NS%mat-sys-on-error-container));
}
.mdc-line-ripple::after {
  transform: scaleX(0);
  opacity: 0;
  z-index: 2;
}
.mdc-text-field--filled .mdc-line-ripple::after {
  border-bottom-width: var(--%NS%mat-form-field-filled-focus-active-indicator-height, 2px);
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--%NS%mat-form-field-filled-focus-active-indicator-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--filled.mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--%NS%mat-form-field-filled-error-focus-active-indicator-color, var(--%NS%mat-sys-error));
}

.mdc-line-ripple--%NS%active::after {
  transform: scaleX(1);
  opacity: 1;
}

.mdc-line-ripple--%NS%deactivating::after {
  opacity: 0;
}

.mdc-text-field--disabled {
  pointer-events: none;
}

.mat-mdc-form-field-textarea-control {
  vertical-align: middle;
  resize: vertical;
  box-sizing: border-box;
  height: auto;
  margin: 0;
  padding: 0;
  border: none;
  overflow: auto;
}

.mat-mdc-form-field-input-control.mat-mdc-form-field-input-control {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font: inherit;
  letter-spacing: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  border: none;
}

.mat-mdc-form-field .mat-mdc-floating-label.mdc-floating-label {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  line-height: normal;
  pointer-events: all;
  will-change: auto;
}

.mat-mdc-form-field:not(.mat-form-field-disabled) .mat-mdc-floating-label.mdc-floating-label {
  cursor: inherit;
}

.mdc-text-field--%NS%no-label:not(.mdc-text-field--textarea) .mat-mdc-form-field-input-control.mdc-text-field__input,
.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control {
  height: auto;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control.mdc-text-field__input[type=color] {
  height: 23px;
}

.mat-mdc-text-field-wrapper {
  height: auto;
  flex: auto;
  will-change: auto;
}

.mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-left: 0;
  --%NS%mat-mdc-form-field-label-offset-x: -16px;
}

.mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

[dir=rtl] .mat-mdc-text-field-wrapper {
  padding-left: 16px;
  padding-right: 16px;
}
[dir=rtl] .mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-left: 0;
}
[dir=rtl] .mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

.mat-form-field-disabled .mdc-text-field__input::placeholder {
  color: var(--%NS%mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-moz-placeholder {
  color: var(--%NS%mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--%NS%mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input:-ms-input-placeholder {
  color: var(--%NS%mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-label-always-float .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
  opacity: 1;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-infix .mat-mdc-floating-label {
  left: auto;
  right: auto;
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-text-field__input {
  display: inline-block;
}

.mat-mdc-form-field .mat-mdc-text-field-wrapper.mdc-text-field .mdc-notched-outline__notch {
  padding-top: 0;
}

.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: 1px solid transparent;
}

[dir=rtl] .mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: none;
  border-right: 1px solid transparent;
}

.mat-mdc-form-field-infix {
  min-height: var(--%NS%mat-form-field-container-height, 56px);
  padding-top: var(--%NS%mat-form-field-filled-with-label-container-padding-top, 24px);
  padding-bottom: var(--%NS%mat-form-field-filled-with-label-container-padding-bottom, 8px);
}
.mdc-text-field--outlined .mat-mdc-form-field-infix, .mdc-text-field--no-label .mat-mdc-form-field-infix {
  padding-top: var(--%NS%mat-form-field-container-vertical-padding, 16px);
  padding-bottom: var(--%NS%mat-form-field-container-vertical-padding, 16px);
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-flex .mat-mdc-floating-label {
  top: calc(var(--%NS%mat-form-field-container-height, 56px) / 2);
}

.mdc-text-field--filled .mat-mdc-floating-label {
  display: var(--%NS%mat-form-field-filled-label-display, block);
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  --%NS%mat-mdc-form-field-label-transform: translateY(calc(calc(6.75px + var(--%NS%mat-form-field-container-height, 56px) / 2) * -1))
    scale(var(--%NS%mat-mdc-form-field-floating-label-scale, 0.75));
  transform: var(--%NS%mat-mdc-form-field-label-transform);
}

@keyframes _mat-form-field-subscript-animation {
  from {
    opacity: 0;
    transform: translateY(-5px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}
.mat-mdc-form-field-subscript-wrapper {
  box-sizing: border-box;
  width: 100%;
  position: relative;
}

.mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-error-wrapper {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  padding: 0 16px;
  opacity: 1;
  transform: translateY(0);
  animation: _mat-form-field-subscript-animation 0ms cubic-bezier(0.55, 0, 0.55, 0.2);
}

.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-error-wrapper {
  position: static;
}

.mat-mdc-form-field-bottom-align::before {
  content: "";
  display: inline-block;
  height: 16px;
}

.mat-mdc-form-field-bottom-align.mat-mdc-form-field-subscript-dynamic-size::before {
  content: unset;
}

.mat-mdc-form-field-hint-end {
  order: 1;
}

.mat-mdc-form-field-hint-wrapper {
  display: flex;
}

.mat-mdc-form-field-hint-spacer {
  flex: 1 0 1em;
}

.mat-mdc-form-field-error {
  display: block;
  color: var(--%NS%mat-form-field-error-text-color, var(--%NS%mat-sys-error));
}

.mat-mdc-form-field-subscript-wrapper,
.mat-mdc-form-field-bottom-align::before {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--%NS%mat-form-field-subscript-text-font, var(--%NS%mat-sys-body-small-font));
  line-height: var(--%NS%mat-form-field-subscript-text-line-height, var(--%NS%mat-sys-body-small-line-height));
  font-size: var(--%NS%mat-form-field-subscript-text-size, var(--%NS%mat-sys-body-small-size));
  letter-spacing: var(--%NS%mat-form-field-subscript-text-tracking, var(--%NS%mat-sys-body-small-tracking));
  font-weight: var(--%NS%mat-form-field-subscript-text-weight, var(--%NS%mat-sys-body-small-weight));
}

.mat-mdc-form-field-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  opacity: 0;
  pointer-events: none;
  background-color: var(--%NS%mat-form-field-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-focus-overlay {
  opacity: var(--%NS%mat-form-field-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-form-field.mat-focused .mat-mdc-form-field-focus-overlay {
  opacity: var(--%NS%mat-form-field-focus-state-layer-opacity, 0);
}

select.mat-mdc-form-field-input-control {
  -moz-appearance: none;
  -webkit-appearance: none;
  background-color: transparent;
  display: inline-flex;
  box-sizing: border-box;
}
select.mat-mdc-form-field-input-control:not(:disabled) {
  cursor: pointer;
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option {
  color: var(--%NS%mat-form-field-select-option-text-color, var(--%NS%mat-sys-neutral10));
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option:disabled {
  color: var(--%NS%mat-form-field-select-disabled-option-text-color, color-mix(in srgb, var(--%NS%mat-sys-neutral10) 38%, transparent));
}

.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  content: "";
  width: 0;
  height: 0;
  border-left: 5px solid transparent;
  border-right: 5px solid transparent;
  border-top: 5px solid;
  position: absolute;
  right: 0;
  top: 50%;
  margin-top: -2.5px;
  pointer-events: none;
  color: var(--%NS%mat-form-field-enabled-select-arrow-color, var(--%NS%mat-sys-on-surface-variant));
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  right: auto;
  left: 0;
}
.mat-mdc-form-field-type-mat-native-select.mat-focused .mat-mdc-form-field-infix::after {
  color: var(--%NS%mat-form-field-focus-select-arrow-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-form-field-type-mat-native-select.mat-form-field-disabled .mat-mdc-form-field-infix::after {
  color: var(--%NS%mat-form-field-disabled-select-arrow-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 15px;
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 0;
  padding-left: 15px;
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill .mat-mdc-text-field-wrapper {
    outline: solid 1px;
  }
}
@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-form-field-disabled .mat-mdc-text-field-wrapper {
    outline-color: GrayText;
  }
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-focused .mat-mdc-text-field-wrapper {
    outline: dashed 3px;
  }
}

@media (forced-colors: active) {
  .mat-mdc-form-field.mat-focused .mdc-notched-outline {
    border: dashed 3px;
  }
}

.mat-mdc-form-field-input-control[type=date], .mat-mdc-form-field-input-control[type=datetime], .mat-mdc-form-field-input-control[type=datetime-local], .mat-mdc-form-field-input-control[type=month], .mat-mdc-form-field-input-control[type=week], .mat-mdc-form-field-input-control[type=time] {
  line-height: 1;
}
.mat-mdc-form-field-input-control::-webkit-datetime-edit {
  line-height: 1;
  padding: 0;
  margin-bottom: -2px;
}

.mat-mdc-form-field {
  --%NS%mat-mdc-form-field-floating-label-scale: 0.75;
  display: inline-flex;
  flex-direction: column;
  min-width: 0;
  text-align: left;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--%NS%mat-form-field-container-text-font, var(--%NS%mat-sys-body-large-font));
  line-height: var(--%NS%mat-form-field-container-text-line-height, var(--%NS%mat-sys-body-large-line-height));
  font-size: var(--%NS%mat-form-field-container-text-size, var(--%NS%mat-sys-body-large-size));
  letter-spacing: var(--%NS%mat-form-field-container-text-tracking, var(--%NS%mat-sys-body-large-tracking));
  font-weight: var(--%NS%mat-form-field-container-text-weight, var(--%NS%mat-sys-body-large-weight));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-floating-label--float-above {
  font-size: calc(var(--%NS%mat-form-field-outlined-label-text-populated-size) * var(--%NS%mat-mdc-form-field-floating-label-scale));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: var(--%NS%mat-form-field-outlined-label-text-populated-size);
}
[dir=rtl] .mat-mdc-form-field {
  text-align: right;
}

.mat-mdc-form-field-flex {
  display: inline-flex;
  align-items: baseline;
  box-sizing: border-box;
  width: 100%;
}

.mat-mdc-text-field-wrapper {
  width: 100%;
  z-index: 0;
}

.mat-mdc-form-field-icon-prefix,
.mat-mdc-form-field-icon-suffix {
  align-self: center;
  line-height: 0;
  pointer-events: auto;
  position: relative;
  z-index: 1;
}
.mat-mdc-form-field-icon-prefix > .mat-icon,
.mat-mdc-form-field-icon-suffix > .mat-icon {
  padding: 0 12px;
  box-sizing: content-box;
}

.mat-mdc-form-field-icon-prefix {
  color: var(--%NS%mat-form-field-leading-icon-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-prefix {
  color: var(--%NS%mat-form-field-disabled-leading-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-icon-suffix {
  color: var(--%NS%mat-form-field-trailing-icon-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-suffix {
  color: var(--%NS%mat-form-field-disabled-trailing-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-invalid .mat-mdc-form-field-icon-suffix {
  color: var(--%NS%mat-form-field-error-trailing-icon-color, var(--%NS%mat-sys-error));
}
.mat-form-field-invalid:not(.mat-focused):not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-icon-suffix {
  color: var(--%NS%mat-form-field-error-hover-trailing-icon-color, var(--%NS%mat-sys-on-error-container));
}
.mat-form-field-invalid.mat-focused .mat-mdc-text-field-wrapper .mat-mdc-form-field-icon-suffix {
  color: var(--%NS%mat-form-field-error-focus-trailing-icon-color, var(--%NS%mat-sys-error));
}

.mat-mdc-form-field-icon-prefix,
[dir=rtl] .mat-mdc-form-field-icon-suffix {
  padding: 0 4px 0 0;
}

.mat-mdc-form-field-icon-suffix,
[dir=rtl] .mat-mdc-form-field-icon-prefix {
  padding: 0 0 0 4px;
}

.mat-mdc-form-field-subscript-wrapper .mat-icon,
.mat-mdc-form-field label .mat-icon {
  width: 1em;
  height: 1em;
  font-size: inherit;
}

.mat-mdc-form-field-infix {
  flex: auto;
  min-width: 0;
  width: 180px;
  position: relative;
  box-sizing: border-box;
}
.mat-mdc-form-field-infix:has(textarea[cols]) {
  width: auto;
}

.mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: -1px;
  -webkit-clip-path: inset(-9em -999em -9em 1px);
  clip-path: inset(-9em -999em -9em 1px);
}
[dir=rtl] .mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: 0;
  margin-right: -1px;
  -webkit-clip-path: inset(-9em 1px -9em -999em);
  clip-path: inset(-9em 1px -9em -999em);
}

.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-floating-label {
  transition: transform 150ms cubic-bezier(0.4, 0, 0.2, 1), color 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input {
  transition: opacity 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-moz-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-webkit-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input:-ms-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field--%NS%filled:not(.mdc-ripple-upgraded):focus .mdc-text-field__ripple::before {
  transition-duration: 75ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-line-ripple::after {
  transition: transform 180ms cubic-bezier(0.4, 0, 0.2, 1), opacity 180ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-error-wrapper {
  animation-duration: 300ms;
}

.mdc-notched-outline .mdc-floating-label {
  max-width: calc(100% + 1px);
}

.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: calc(133.3333333333% + 1px);
}
`],encapsulation:2})}return t})();var Tn=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({imports:[Uu,cr,ve]})}return t})();function TE(t){return Error(`Unable to find icon with the name "${t}"`)}function SF(){return Error("Could not find HttpClient for use with Angular Material icons. Please add provideHttpClient() to your providers.")}function NE(t){return Error(`The URL provided to MatIconRegistry was not trusted as a resource URL via Angular's DomSanitizer. Attempted URL was "${t}".`)}function kE(t){return Error(`The literal provided to MatIconRegistry was not trusted as safe HTML by Angular's DomSanitizer. Attempted literal was "${t}".`)}var Ei=class{url;svgText;options;svgElement=null;constructor(n,e,i){this.url=n,this.svgText=e,this.options=i}},RE=(()=>{class t{_httpClient;_sanitizer;_errorHandler;_document;_svgIconConfigs=new Map;_iconSetConfigs=new Map;_cachedIconsByUrl=new Map;_inProgressUrlFetches=new Map;_fontCssClassesByAlias=new Map;_resolvers=[];_defaultFontSetClass=["material-icons","mat-ligature-font"];constructor(e,i,r,o){this._httpClient=e,this._sanitizer=i,this._errorHandler=o,this._document=r}addSvgIcon(e,i,r){return this.addSvgIconInNamespace("",e,i,r)}addSvgIconLiteral(e,i,r){return this.addSvgIconLiteralInNamespace("",e,i,r)}addSvgIconInNamespace(e,i,r,o){return this._addSvgIconConfig(e,i,new Ei(r,null,o))}addSvgIconResolver(e){return this._resolvers.push(e),this}addSvgIconLiteralInNamespace(e,i,r,o){let s=this._sanitizer.sanitize(st.HTML,r);if(!s)throw kE(r);let a=so(s);return this._addSvgIconConfig(e,i,new Ei("",a,o))}addSvgIconSet(e,i){return this.addSvgIconSetInNamespace("",e,i)}addSvgIconSetLiteral(e,i){return this.addSvgIconSetLiteralInNamespace("",e,i)}addSvgIconSetInNamespace(e,i,r){return this._addSvgIconSetConfig(e,new Ei(i,null,r))}addSvgIconSetLiteralInNamespace(e,i,r){let o=this._sanitizer.sanitize(st.HTML,i);if(!o)throw kE(i);let s=so(o);return this._addSvgIconSetConfig(e,new Ei("",s,r))}registerFontClassAlias(e,i=e){return this._fontCssClassesByAlias.set(e,i),this}classNameForFontAlias(e){return this._fontCssClassesByAlias.get(e)||e}setDefaultFontSetClass(...e){return this._defaultFontSetClass=e,this}getDefaultFontSetClass(){return this._defaultFontSetClass}getSvgIconFromUrl(e){let i=this._sanitizer.sanitize(st.RESOURCE_URL,e);if(!i)throw NE(e);let r=this._cachedIconsByUrl.get(i);return r?Y(cf(r)):this._loadSvgIconFromConfig(new Ei(e,null)).pipe(dt(o=>this._cachedIconsByUrl.set(i,o)),oe(o=>cf(o)))}getNamedSvgIcon(e,i=""){let r=AE(i,e),o=this._svgIconConfigs.get(r);if(o)return this._getSvgFromConfig(o);if(o=this._getIconConfigFromResolvers(i,e),o)return this._svgIconConfigs.set(r,o),this._getSvgFromConfig(o);let s=this._iconSetConfigs.get(i);return s?this._getSvgFromIconSetConfigs(e,s):Ws(TE(r))}ngOnDestroy(){this._resolvers=[],this._svgIconConfigs.clear(),this._iconSetConfigs.clear(),this._cachedIconsByUrl.clear()}_getSvgFromConfig(e){return e.svgText?Y(cf(this._svgElementFromConfig(e))):this._loadSvgIconFromConfig(e).pipe(oe(i=>cf(i)))}_getSvgFromIconSetConfigs(e,i){let r=this._extractIconWithNameFromAnySet(e,i);if(r)return Y(r);let o=i.filter(s=>!s.svgText).map(s=>this._loadSvgIconSetFromConfig(s).pipe(wr(a=>{let c=`Loading icon set URL: ${this._sanitizer.sanitize(st.RESOURCE_URL,s.url)} failed: ${a.message}`;return this._errorHandler.handleError(new Error(c)),Y(null)})));return Zs(o).pipe(oe(()=>{let s=this._extractIconWithNameFromAnySet(e,i);if(!s)throw TE(e);return s}))}_extractIconWithNameFromAnySet(e,i){for(let r=i.length-1;r>=0;r--){let o=i[r];if(o.svgText&&o.svgText.toString().indexOf(e)>-1){let s=this._svgElementFromConfig(o),a=this._extractSvgIconFromSet(s,e,o.options);if(a)return a}}return null}_loadSvgIconFromConfig(e){return this._fetchIcon(e).pipe(dt(i=>e.svgText=i),oe(()=>this._svgElementFromConfig(e)))}_loadSvgIconSetFromConfig(e){return e.svgText?Y(null):this._fetchIcon(e).pipe(dt(i=>e.svgText=i))}_extractSvgIconFromSet(e,i,r){let o=e.querySelector(`[id="${i}"]`);if(!o)return null;let s=o.cloneNode(!0);if(s.removeAttribute("id"),s.nodeName.toLowerCase()==="svg")return this._setSvgAttributes(s,r);if(s.nodeName.toLowerCase()==="symbol")return this._setSvgAttributes(this._toSvgElement(s),r);let a=this._svgElementFromString(so("<svg></svg>"));return a.appendChild(s),this._setSvgAttributes(a,r)}_svgElementFromString(e){let i=this._document.createElement("DIV");i.innerHTML=e;let r=i.querySelector("svg");if(!r)throw Error("<svg> tag not found");return r}_toSvgElement(e){let i=this._svgElementFromString(so("<svg></svg>")),r=e.attributes;for(let o=0;o<r.length;o++){let{name:s,value:a}=r[o];s!=="id"&&i.setAttribute(s,a)}for(let o=0;o<e.childNodes.length;o++)e.childNodes[o].nodeType===this._document.ELEMENT_NODE&&i.appendChild(e.childNodes[o].cloneNode(!0));return i}_setSvgAttributes(e,i){return e.setAttribute("fit",""),e.setAttribute("height","100%"),e.setAttribute("width","100%"),e.setAttribute("preserveAspectRatio","xMidYMid meet"),e.setAttribute("focusable","false"),i&&i.viewBox&&e.setAttribute("viewBox",i.viewBox),e}_fetchIcon(e){let{url:i,options:r}=e,o=r?.withCredentials??!1;if(!this._httpClient)throw SF();if(i==null)throw Error(`Cannot fetch icon from URL "${i}".`);let s=this._sanitizer.sanitize(st.RESOURCE_URL,i);if(!s)throw NE(i);let a=this._inProgressUrlFetches.get(s);if(a)return a;let l=this._httpClient.get(s,{responseType:"text",withCredentials:o}).pipe(oe(c=>so(c)),xr(()=>this._inProgressUrlFetches.delete(s)),Ks());return this._inProgressUrlFetches.set(s,l),l}_addSvgIconConfig(e,i,r){return this._svgIconConfigs.set(AE(e,i),r),this}_addSvgIconSetConfig(e,i){let r=this._iconSetConfigs.get(e);return r?r.push(i):this._iconSetConfigs.set(e,[i]),this}_svgElementFromConfig(e){if(!e.svgElement){let i=this._svgElementFromString(e.svgText);this._setSvgAttributes(i,e.options),e.svgElement=i}return e.svgElement}_getIconConfigFromResolvers(e,i){for(let r=0;r<this._resolvers.length;r++){let o=this._resolvers[r](i,e);if(o)return CF(o)?new Ei(o.url,null,o.options):new Ei(o,null)}}static \u0275fac=function(i){return new(i||t)(A(lg,8),A(za),A(L,8),A(Ot))};static \u0275prov=G({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function cf(t){return t.cloneNode(!0)}function AE(t,n){return t+":"+n}function CF(t){return!!(t.url&&t.options)}var DF=["*"],EF=new b("MAT_ICON_DEFAULT_OPTIONS"),wF=new b("mat-icon-location",{providedIn:"root",factory:()=>{let t=d(L),n=t?t.location:null;return{getPathname:()=>n?n.pathname+n.search:""}}}),OE=["clip-path","color-profile","src","cursor","fill","filter","marker","marker-start","marker-mid","marker-end","mask","stroke"],xF=OE.map(t=>`[${t}]`).join(", "),IF=/^url\(['"]?#(.*?)['"]?\)$/,Yn=(()=>{class t{_elementRef=d(O);_iconRegistry=d(RE);_location=d(wF);_errorHandler=d(Ot);_defaultColor;get color(){return this._color||this._defaultColor}set color(e){this._color=e}_color;inline=!1;get svgIcon(){return this._svgIcon}set svgIcon(e){e!==this._svgIcon&&(e?this._updateSvgIcon(e):this._svgIcon&&this._clearSvgElement(),this._svgIcon=e)}_svgIcon;get fontSet(){return this._fontSet}set fontSet(e){let i=this._cleanupFontValue(e);i!==this._fontSet&&(this._fontSet=i,this._updateFontIconClasses())}_fontSet;get fontIcon(){return this._fontIcon}set fontIcon(e){let i=this._cleanupFontValue(e);i!==this._fontIcon&&(this._fontIcon=i,this._updateFontIconClasses())}_fontIcon;_previousFontSetClass=[];_previousFontIconClass;_svgName=null;_svgNamespace=null;_previousPath;_elementsWithExternalReferences;_currentIconFetch=pe.EMPTY;constructor(){let e=d(new _n("aria-hidden"),{optional:!0}),i=d(EF,{optional:!0});i&&(i.color&&(this.color=this._defaultColor=i.color),i.fontSet&&(this.fontSet=i.fontSet)),e||this._elementRef.nativeElement.setAttribute("aria-hidden","true")}_splitIconName(e){if(!e)return["",""];let i=e.split(":");switch(i.length){case 1:return["",i[0]];case 2:return i;default:throw Error(`Invalid icon name: "${e}"`)}}ngOnInit(){this._updateFontIconClasses()}ngAfterViewChecked(){let e=this._elementsWithExternalReferences;if(e&&e.size){let i=this._location.getPathname();i!==this._previousPath&&(this._previousPath=i,this._prependPathToReferences(i))}}ngOnDestroy(){this._currentIconFetch.unsubscribe(),this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear()}_usingFontIcon(){return!this.svgIcon}_setSvgElement(e){this._clearSvgElement();let i=this._location.getPathname();this._previousPath=i,this._cacheChildrenWithExternalReferences(e),this._prependPathToReferences(i),this._elementRef.nativeElement.appendChild(e)}_clearSvgElement(){let e=this._elementRef.nativeElement,i=e.childNodes.length;for(this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear();i--;){let r=e.childNodes[i];(r.nodeType!==1||r.nodeName.toLowerCase()==="svg")&&r.remove()}}_updateFontIconClasses(){if(!this._usingFontIcon())return;let e=this._elementRef.nativeElement,i=(this.fontSet?this._iconRegistry.classNameForFontAlias(this.fontSet).split(/ +/):this._iconRegistry.getDefaultFontSetClass()).filter(r=>r.length>0);this._previousFontSetClass.forEach(r=>e.classList.remove(r)),i.forEach(r=>e.classList.add(r)),this._previousFontSetClass=i,this.fontIcon!==this._previousFontIconClass&&!i.includes("mat-ligature-font")&&(this._previousFontIconClass&&e.classList.remove(this._previousFontIconClass),this.fontIcon&&e.classList.add(this.fontIcon),this._previousFontIconClass=this.fontIcon)}_cleanupFontValue(e){return typeof e=="string"?e.trim().split(" ")[0]:e}_prependPathToReferences(e){let i=this._elementsWithExternalReferences;i&&i.forEach((r,o)=>{r.forEach(s=>{o.setAttribute(s.name,`url('${e}#${s.value}')`)})})}_cacheChildrenWithExternalReferences(e){let i=e.querySelectorAll(xF),r=this._elementsWithExternalReferences=this._elementsWithExternalReferences||new Map;for(let o=0;o<i.length;o++)OE.forEach(s=>{let a=i[o],l=a.getAttribute(s),c=l?l.match(IF):null;if(c){let u=r.get(a);u||(u=[],r.set(a,u)),u.push({name:s,value:c[1]})}})}_updateSvgIcon(e){if(this._svgNamespace=null,this._svgName=null,this._currentIconFetch.unsubscribe(),e){let[i,r]=this._splitIconName(e);i&&(this._svgNamespace=i),r&&(this._svgName=r),this._currentIconFetch=this._iconRegistry.getNamedSvgIcon(r,i).pipe(Ae(1)).subscribe(o=>this._setSvgElement(o),o=>{let s=`Error retrieving icon ${i}:${r}! ${o.message}`;this._errorHandler.handleError(new Error(s))})}}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-icon"]],hostAttrs:["role","img",1,"mat-icon","notranslate"],hostVars:10,hostBindings:function(i,r){i&2&&(re("data-mat-icon-type",r._usingFontIcon()?"font":"svg")("data-mat-icon-name",r._svgName||r.fontIcon)("data-mat-icon-namespace",r._svgNamespace||r.fontSet)("fontIcon",r._usingFontIcon()?r.fontIcon:null),wt(r.color?"mat-"+r.color:""),z("mat-icon-inline",r.inline)("mat-icon-no-color",r.color!=="primary"&&r.color!=="accent"&&r.color!=="warn"))},inputs:{color:"color",inline:[2,"inline","inline",P],svgIcon:"svgIcon",fontSet:"fontSet",fontIcon:"fontIcon"},exportAs:["matIcon"],ngContentSelectors:DF,decls:1,vars:0,template:function(i,r){i&1&&(Pe(),he(0))},styles:[`mat-icon, mat-icon.mat-primary, mat-icon.mat-accent, mat-icon.mat-warn {
  color: var(--%NS%mat-icon-color, inherit);
}

.mat-icon {
  -webkit-user-select: none;
  user-select: none;
  background-repeat: no-repeat;
  display: inline-block;
  fill: currentColor;
  height: 24px;
  width: 24px;
  overflow: hidden;
}
.mat-icon.mat-icon-inline {
  font-size: inherit;
  height: inherit;
  line-height: inherit;
  width: inherit;
}
.mat-icon.mat-ligature-font[fontIcon]::before {
  content: attr(fontIcon);
}

[dir=rtl] .mat-icon-rtl-mirror {
  transform: scale(-1, 1);
}

.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-prefix .mat-icon,
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-suffix .mat-icon {
  display: block;
}
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-prefix .mat-icon-button .mat-icon,
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-suffix .mat-icon-button .mat-icon {
  margin: auto;
}
`],encapsulation:2})}return t})(),Zn=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({imports:[ve]})}return t})();var MF=[[["mat-icon"],["","matMenuItemIcon",""]],"*"],TF=["mat-icon, [matMenuItemIcon]","*"];function NF(t,n){t&1&&(Fi(),m(0,"svg",2),ie(1,"polygon",3),g())}var kF=["*"];function AF(t,n){if(t&1){let e=Le();lt(0,"div",0),Ko("click",function(){ee(e);let r=w();return te(r.closed.emit("click"))})("animationstart",function(r){ee(e);let o=w();return te(o._onAnimationStart(r.animationName))})("animationend",function(r){ee(e);let o=w();return te(o._onAnimationDone(r.animationName))})("animationcancel",function(r){ee(e);let o=w();return te(o._onAnimationDone(r.animationName))}),lt(1,"div",1),he(2),_t()()}if(t&2){let e=w();wt(e._classList),z("mat-menu-panel-animations-disabled",e._animationsDisabled)("mat-menu-panel-exit-animation",e._panelAnimationState==="void")("mat-menu-panel-animating",e._isAnimating()),Et("id",e.panelId),re("aria-label",e.ariaLabel||null)("aria-labelledby",e.ariaLabelledby||null)("aria-describedby",e.ariaDescribedby||null)}}var Dv=new b("MAT_MENU_PANEL"),kl=(()=>{class t{_elementRef=d(O);_document=d(L);_focusMonitor=d(rn);_parentMenu=d(Dv,{optional:!0});_changeDetectorRef=d(Oe);role="menuitem";disabled=!1;disableRipple=!1;_hovered=new E;_focused=new E;_highlighted=!1;_triggersSubmenu=!1;constructor(){d(ze).load(qn),this._parentMenu?.addItem?.(this)}focus(e,i){this._focusMonitor&&e?this._focusMonitor.focusVia(this._getHostElement(),e,i):this._getHostElement().focus(i),this._focused.next(this)}ngAfterViewInit(){this._focusMonitor&&this._focusMonitor.monitor(this._elementRef,!1)}ngOnDestroy(){this._focusMonitor&&this._focusMonitor.stopMonitoring(this._elementRef),this._parentMenu&&this._parentMenu.removeItem&&this._parentMenu.removeItem(this),this._hovered.complete(),this._focused.complete()}_getTabIndex(){return this.disabled?"-1":"0"}_getHostElement(){return this._elementRef.nativeElement}_checkDisabled(e){this.disabled&&(e.preventDefault(),e.stopPropagation())}_handleMouseEnter(){this._hovered.next(this)}getLabel(){let e=this._elementRef.nativeElement.cloneNode(!0),i=e.querySelectorAll("mat-icon, .material-icons");for(let r=0;r<i.length;r++)i[r].remove();return e.textContent?.trim()||""}_setHighlighted(e){this._highlighted=e,this._changeDetectorRef.markForCheck()}_setTriggersSubmenu(e){this._triggersSubmenu=e,this._changeDetectorRef.markForCheck()}_hasFocus(){return this._document&&this._document.activeElement===this._getHostElement()}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["","mat-menu-item",""]],hostAttrs:[1,"mat-mdc-menu-item","mat-focus-indicator"],hostVars:8,hostBindings:function(i,r){i&1&&D("click",function(s){return r._checkDisabled(s)})("mouseenter",function(){return r._handleMouseEnter()}),i&2&&(re("role",r.role)("tabindex",r._getTabIndex())("aria-disabled",r.disabled)("disabled",r.disabled||null),z("mat-mdc-menu-item-highlighted",r._highlighted)("mat-mdc-menu-item-submenu-trigger",r._triggersSubmenu))},inputs:{role:"role",disabled:[2,"disabled","disabled",P],disableRipple:[2,"disableRipple","disableRipple",P]},exportAs:["matMenuItem"],ngContentSelectors:TF,decls:5,vars:3,consts:[[1,"mat-mdc-menu-item-text"],["matRipple","",1,"mat-mdc-menu-ripple",3,"matRippleDisabled","matRippleTrigger"],["viewBox","0 0 5 10","focusable","false","aria-hidden","true",1,"mat-mdc-menu-submenu-icon"],["points","0,0 5,5 0,10"]],template:function(i,r){i&1&&(Pe(MF),he(0),m(1,"span",0),he(2,1),g(),ie(3,"div",1),V(4,NF,2,0,":svg:svg",2)),i&2&&(_(3),U("matRippleDisabled",r.disableRipple||r.disabled)("matRippleTrigger",r._getHostElement()),_(),B(r._triggersSubmenu?4:-1))},dependencies:[sr],encapsulation:2})}return t})();var RF=new b("MatMenuContent");var OF=new b("mat-menu-default-options",{providedIn:"root",factory:()=>({overlapTrigger:!1,xPosition:"after",yPosition:"below",backdropClass:"cdk-overlay-transparent-backdrop"})}),Cv="_mat-menu-enter",df="_mat-menu-exit",xs=(()=>{class t{_elementRef=d(O);_changeDetectorRef=d(Oe);_injector=d(R);_keyManager;_xPosition;_yPosition;_firstItemFocusRef;_exitFallbackTimeout;_animationsDisabled=Ce();_allItems;_directDescendantItems=new ai;_classList={};_panelAnimationState="void";_animationDone=new E;_isAnimating=j(!1);parentMenu;direction;overlayPanelClass;backdropClass;ariaLabel;ariaLabelledby;ariaDescribedby;get xPosition(){return this._xPosition}set xPosition(e){this._xPosition=e,this.setPositionClasses()}get yPosition(){return this._yPosition}set yPosition(e){this._yPosition=e,this.setPositionClasses()}templateRef;items;lazyContent;overlapTrigger=!1;hasBackdrop;get panelClass(){return this._previousPanelClass}set panelClass(e){let i=this._previousPanelClass,r=v({},this._classList);i&&i.length&&i.split(" ").forEach(o=>{r[o]=!1}),this._previousPanelClass=e,e&&e.length&&(e.split(" ").forEach(o=>{r[o]=!0}),this._elementRef.nativeElement.className=""),this._classList=r}_previousPanelClass="";get classList(){return this.panelClass}set classList(e){this.panelClass=e}closed=new W;close=this.closed;panelId=d(xe).getId("mat-menu-panel-");constructor(){let e=d(OF);this.overlayPanelClass=e.overlayPanelClass||"",this._xPosition=e.xPosition,this._yPosition=e.yPosition,this.backdropClass=e.backdropClass,this.overlapTrigger=e.overlapTrigger,this.hasBackdrop=e.hasBackdrop}ngOnInit(){this.setPositionClasses()}ngAfterContentInit(){this._updateDirectDescendants(),this._keyManager=new El(this._directDescendantItems).withWrap().withTypeAhead().withHomeAndEnd(),this._keyManager.tabOut.subscribe(()=>this.closed.emit("tab")),this._directDescendantItems.changes.pipe(nt(this._directDescendantItems),Xe(e=>Nt(...e.map(i=>i._focused)))).subscribe(e=>this._keyManager.updateActiveItem(e)),this._directDescendantItems.changes.subscribe(e=>{let i=this._keyManager;if(this._panelAnimationState==="enter"&&i.activeItem?._hasFocus()){let r=e.toArray(),o=Math.max(0,Math.min(r.length-1,i.activeItemIndex||0));r[o]&&!r[o].disabled?i.setActiveItem(o):i.setNextItemActive()}})}ngOnDestroy(){this._keyManager?.destroy(),this._directDescendantItems.destroy(),this.closed.complete(),this._firstItemFocusRef?.destroy(),clearTimeout(this._exitFallbackTimeout)}_hovered(){return this._directDescendantItems.changes.pipe(nt(this._directDescendantItems),Xe(i=>Nt(...i.map(r=>r._hovered))))}addItem(e){}removeItem(e){}_handleKeydown(e){let i=e.keyCode,r=this._keyManager;switch(i){case 27:$e(e)||(e.preventDefault(),this.closed.emit("keydown"));break;case 37:this.parentMenu&&this.direction==="ltr"&&this.closed.emit("keydown");break;case 39:this.parentMenu&&this.direction==="rtl"&&this.closed.emit("keydown");break;default:(i===38||i===40)&&r.setFocusOrigin("keyboard"),r.onKeydown(e);return}}focusFirstItem(e="program"){this._firstItemFocusRef?.destroy(),this._firstItemFocusRef=Qe(()=>{let i=this._resolvePanel();if(!i||!i.contains(document.activeElement)){let r=this._keyManager;r.setFocusOrigin(e).setFirstItemActive(),!r.activeItem&&i&&i.focus()}},{injector:this._injector})}resetActiveItem(){this._keyManager.setActiveItem(-1)}setElevation(e){}setPositionClasses(e=this.xPosition,i=this.yPosition){this._classList=k(v({},this._classList),{"mat-menu-before":e==="before","mat-menu-after":e==="after","mat-menu-above":i==="above","mat-menu-below":i==="below"}),this._changeDetectorRef.markForCheck()}_onAnimationDone(e){let i=e===df;(i||e===Cv)&&(i&&(clearTimeout(this._exitFallbackTimeout),this._exitFallbackTimeout=void 0),this._animationDone.next(i?"void":"enter"),this._isAnimating.set(!1))}_onAnimationStart(e){(e===Cv||e===df)&&this._isAnimating.set(!0)}_setIsOpen(e){if(this._panelAnimationState=e?"enter":"void",e){if(this._keyManager.activeItemIndex===0){let i=this._resolvePanel();i&&(i.scrollTop=0)}}else this._animationsDisabled||(this._exitFallbackTimeout=setTimeout(()=>this._onAnimationDone(df),200));this._animationsDisabled&&setTimeout(()=>{this._onAnimationDone(e?Cv:df)}),this._changeDetectorRef.markForCheck()}_updateDirectDescendants(){this._allItems.changes.pipe(nt(this._allItems)).subscribe(e=>{this._directDescendantItems.reset(e.filter(i=>i._parentMenu===this)),this._directDescendantItems.notifyOnChanges()})}_resolvePanel(){let e=null;return this._directDescendantItems.length&&(e=this._directDescendantItems.first._getHostElement().closest('[role="menu"]')),e}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-menu"]],contentQueries:function(i,r,o){if(i&1&&zn(o,RF,5)(o,kl,5)(o,kl,4),i&2){let s;ce(s=de())&&(r.lazyContent=s.first),ce(s=de())&&(r._allItems=s),ce(s=de())&&(r.items=s)}},viewQuery:function(i,r){if(i&1&&Ue(Ct,5),i&2){let o;ce(o=de())&&(r.templateRef=o.first)}},hostVars:3,hostBindings:function(i,r){i&2&&re("aria-label",null)("aria-labelledby",null)("aria-describedby",null)},inputs:{backdropClass:"backdropClass",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],xPosition:"xPosition",yPosition:"yPosition",overlapTrigger:[2,"overlapTrigger","overlapTrigger",P],hasBackdrop:[2,"hasBackdrop","hasBackdrop",e=>e==null?null:P(e)],panelClass:[0,"class","panelClass"],classList:"classList"},outputs:{closed:"closed",close:"close"},exportAs:["matMenu"],features:[Be([{provide:Dv,useExisting:t}])],ngContentSelectors:kF,decls:1,vars:0,consts:[["tabindex","-1","role","menu",1,"mat-mdc-menu-panel",3,"click","animationstart","animationend","animationcancel","id"],[1,"mat-mdc-menu-content"]],template:function(i,r){i&1&&(Pe(),zd(0,AF,3,12,"ng-template"))},styles:[`mat-menu {
  display: none;
}

.mat-mdc-menu-content {
  margin: 0;
  padding: 8px 0;
  outline: 0;
}
.mat-mdc-menu-content,
.mat-mdc-menu-content .mat-mdc-menu-item .mat-mdc-menu-item-text {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  flex: 1;
  white-space: normal;
  font-family: var(--%NS%mat-menu-item-label-text-font, var(--%NS%mat-sys-label-large-font));
  line-height: var(--%NS%mat-menu-item-label-text-line-height, var(--%NS%mat-sys-label-large-line-height));
  font-size: var(--%NS%mat-menu-item-label-text-size, var(--%NS%mat-sys-label-large-size));
  letter-spacing: var(--%NS%mat-menu-item-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
  font-weight: var(--%NS%mat-menu-item-label-text-weight, var(--%NS%mat-sys-label-large-weight));
}

@keyframes _mat-menu-enter {
  from {
    opacity: 0;
    transform: scale(0.8);
  }
  to {
    opacity: 1;
    transform: none;
  }
}
@keyframes _mat-menu-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-menu-panel {
  min-width: 112px;
  max-width: 280px;
  overflow: auto;
  box-sizing: border-box;
  outline: 0;
  animation: _mat-menu-enter 120ms cubic-bezier(0, 0, 0.2, 1);
  border-radius: var(--%NS%mat-menu-container-shape, var(--%NS%mat-sys-corner-extra-small));
  background-color: var(--%NS%mat-menu-container-color, var(--%NS%mat-sys-surface-container));
  box-shadow: var(--%NS%mat-menu-container-elevation-shadow, 0px 3px 1px -2px rgba(0, 0, 0, 0.2), 0px 2px 2px 0px rgba(0, 0, 0, 0.14), 0px 1px 5px 0px rgba(0, 0, 0, 0.12));
  will-change: transform, opacity;
}
.mat-mdc-menu-panel.mat-menu-panel-exit-animation {
  animation: _mat-menu-exit 100ms 25ms linear forwards;
}
.mat-mdc-menu-panel.mat-menu-panel-animations-disabled {
  animation: none;
}
.mat-mdc-menu-panel.mat-menu-panel-animating {
  pointer-events: none;
}
.mat-mdc-menu-panel.mat-menu-panel-animating:has(.mat-mdc-menu-content:empty) {
  display: none;
}
@media (forced-colors: active) {
  .mat-mdc-menu-panel {
    outline: solid 1px;
  }
}
.mat-mdc-menu-panel .mat-divider {
  border-top-color: var(--%NS%mat-menu-divider-color, var(--%NS%mat-sys-surface-variant));
  margin-bottom: var(--%NS%mat-menu-divider-bottom-spacing, 8px);
  margin-top: var(--%NS%mat-menu-divider-top-spacing, 8px);
}

.mat-mdc-menu-item {
  display: flex;
  position: relative;
  align-items: center;
  justify-content: flex-start;
  overflow: hidden;
  padding: 0;
  cursor: pointer;
  width: 100%;
  text-align: left;
  box-sizing: border-box;
  color: inherit;
  font-size: inherit;
  background: none;
  text-decoration: none;
  margin: 0;
  min-height: 48px;
  padding-left: var(--%NS%mat-menu-item-leading-spacing, 12px);
  padding-right: var(--%NS%mat-menu-item-trailing-spacing, 12px);
  -webkit-user-select: none;
  user-select: none;
  cursor: pointer;
  outline: none;
  border: none;
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-menu-item::-moz-focus-inner {
  border: 0;
}
[dir=rtl] .mat-mdc-menu-item {
  padding-left: var(--%NS%mat-menu-item-trailing-spacing, 12px);
  padding-right: var(--%NS%mat-menu-item-leading-spacing, 12px);
}
.mat-mdc-menu-item:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding-left: var(--%NS%mat-menu-item-with-icon-leading-spacing, 12px);
  padding-right: var(--%NS%mat-menu-item-with-icon-trailing-spacing, 12px);
}
[dir=rtl] .mat-mdc-menu-item:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding-left: var(--%NS%mat-menu-item-with-icon-trailing-spacing, 12px);
  padding-right: var(--%NS%mat-menu-item-with-icon-leading-spacing, 12px);
}
.mat-mdc-menu-item, .mat-mdc-menu-item:visited, .mat-mdc-menu-item:link {
  color: var(--%NS%mat-menu-item-label-text-color, var(--%NS%mat-sys-on-surface));
}
.mat-mdc-menu-item .mat-icon-no-color,
.mat-mdc-menu-item .mat-mdc-menu-submenu-icon {
  color: var(--%NS%mat-menu-item-icon-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-menu-item[disabled] {
  cursor: default;
  opacity: 0.38;
}
.mat-mdc-menu-item[disabled]::after {
  display: block;
  position: absolute;
  content: "";
  top: 0;
  left: 0;
  bottom: 0;
  right: 0;
}
.mat-mdc-menu-item:focus {
  outline: 0;
}
.mat-mdc-menu-item .mat-icon {
  flex-shrink: 0;
  margin-right: var(--%NS%mat-menu-item-spacing, 12px);
  height: var(--%NS%mat-menu-item-icon-size, 24px);
  width: var(--%NS%mat-menu-item-icon-size, 24px);
}
[dir=rtl] .mat-mdc-menu-item {
  text-align: right;
}
[dir=rtl] .mat-mdc-menu-item .mat-icon {
  margin-right: 0;
  margin-left: var(--%NS%mat-menu-item-spacing, 12px);
}
.mat-mdc-menu-item:not([disabled]):hover {
  background-color: var(--%NS%mat-menu-item-hover-state-layer-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) calc(var(--%NS%mat-sys-hover-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-menu-item:not([disabled]).cdk-program-focused, .mat-mdc-menu-item:not([disabled]).cdk-keyboard-focused, .mat-mdc-menu-item:not([disabled]).mat-mdc-menu-item-highlighted {
  background-color: var(--%NS%mat-menu-item-focus-state-layer-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) calc(var(--%NS%mat-sys-focus-state-layer-opacity) * 100%), transparent));
}
@media (forced-colors: active) {
  .mat-mdc-menu-item {
    margin-top: 1px;
  }
}

.mat-mdc-menu-submenu-icon {
  width: var(--%NS%mat-menu-item-icon-size, 24px);
  height: 10px;
  fill: currentColor;
  padding-left: var(--%NS%mat-menu-item-spacing, 12px);
}
[dir=rtl] .mat-mdc-menu-submenu-icon {
  padding-right: var(--%NS%mat-menu-item-spacing, 12px);
  padding-left: 0;
}
[dir=rtl] .mat-mdc-menu-submenu-icon polygon {
  transform: scaleX(-1);
  transform-origin: center;
}
@media (forced-colors: active) {
  .mat-mdc-menu-submenu-icon {
    fill: CanvasText;
  }
}

.mat-mdc-menu-item .mat-mdc-menu-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}
`],encapsulation:2})}return t})(),PF=new b("mat-menu-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(R);return()=>Si(t)}});var ws=new WeakMap,FF=(()=>{class t{_canHaveBackdrop;_element=d(O);_viewContainerRef=d(bt);_menuItemInstance=d(kl,{optional:!0,self:!0});_dir=d(ct,{optional:!0});_focusMonitor=d(rn);_ngZone=d(N);_injector=d(R);_scrollStrategy=d(PF);_changeDetectorRef=d(Oe);_animationsDisabled=Ce();_portal;_overlayRef=null;_menuOpen=!1;_closingActionsSubscription=pe.EMPTY;_menuCloseSubscription=pe.EMPTY;_pendingRemoval;_parentMaterialMenu;_parentInnerPadding;_openedBy=void 0;get _menu(){return this._menuInternal}set _menu(e){e!==this._menuInternal&&(this._menuInternal=e,this._menuCloseSubscription.unsubscribe(),e?(this._parentMaterialMenu,this._menuCloseSubscription=e.close.subscribe(i=>{this._destroyMenu(i),(i==="click"||i==="tab")&&this._parentMaterialMenu&&this._parentMaterialMenu.closed.emit(i)})):this._destroyMenu(),this._menuItemInstance?._setTriggersSubmenu(this._triggersSubmenu()))}_menuInternal=null;constructor(e){this._canHaveBackdrop=e;let i=d(Dv,{optional:!0});this._parentMaterialMenu=i instanceof xs?i:void 0}ngOnDestroy(){this._menu&&this._ownsMenu(this._menu)&&ws.delete(this._menu),this._pendingRemoval?.unsubscribe(),this._menuCloseSubscription.unsubscribe(),this._closingActionsSubscription.unsubscribe(),this._overlayRef&&(this._overlayRef.dispose(),this._overlayRef=null)}get menuOpen(){return this._menuOpen}get dir(){return this._dir&&this._dir.value==="rtl"?"rtl":"ltr"}_triggersSubmenu(){return!!(this._menuItemInstance&&this._parentMaterialMenu&&this._menu)}_closeMenu(){this._menu?.close.emit()}_openMenu(e){if(this._triggerIsAriaDisabled())return;let i=this._menu;if(this._menuOpen||!i)return;this._pendingRemoval?.unsubscribe();let r=ws.get(i);ws.set(i,this),r&&r!==this&&r._closeMenu();let o=this._createOverlay(i),s=o.getConfig(),a=s.positionStrategy;this._setPosition(i,a),this._canHaveBackdrop?s.hasBackdrop=i.hasBackdrop==null?!this._triggersSubmenu():i.hasBackdrop:s.hasBackdrop=i.hasBackdrop??!1,o.hasAttached()||(o.attach(this._getPortal(i)),i.lazyContent?.attach(this.menuData)),this._closingActionsSubscription=this._menuClosingActions().subscribe(()=>this._closeMenu()),i.parentMenu=this._triggersSubmenu()?this._parentMaterialMenu:void 0,i.direction=this.dir,e&&i.focusFirstItem(this._openedBy||"program"),this._setIsMenuOpen(!0),i instanceof xs&&(i._setIsOpen(!0),i._directDescendantItems.changes.pipe(ye(i.close)).subscribe(()=>{a.withLockedPosition(!1).reapplyLastPosition(),a.withLockedPosition(!0)}))}focus(e,i){this._focusMonitor&&e?this._focusMonitor.focusVia(this._element,e,i):this._element.nativeElement.focus(i)}_destroyMenu(e){let i=this._overlayRef,r=this._menu;!i||!this.menuOpen||(this._closingActionsSubscription.unsubscribe(),this._pendingRemoval?.unsubscribe(),r instanceof xs&&this._ownsMenu(r)?(this._pendingRemoval=r._animationDone.pipe(Ae(1)).subscribe(()=>{i.detach(),ws.has(r)||r.lazyContent?.detach()}),r._setIsOpen(!1)):(i.detach(),r?.lazyContent?.detach()),r&&this._ownsMenu(r)&&ws.delete(r),this.restoreFocus&&(e==="keydown"||!this._openedBy||!this._triggersSubmenu())&&this.focus(this._openedBy),this._openedBy=void 0,this._setIsMenuOpen(!1))}_setIsMenuOpen(e){e!==this._menuOpen&&(this._menuOpen=e,this._menuOpen?this.menuOpened.emit():this.menuClosed.emit(),this._triggersSubmenu()&&this._menuItemInstance._setHighlighted(e),this._changeDetectorRef.markForCheck())}_createOverlay(e){if(!this._overlayRef){let i=this._getOverlayConfig(e);this._subscribeToPositions(e,i.positionStrategy),this._overlayRef=In(this._injector,i),this._overlayRef.keydownEvents().subscribe(r=>{this._menu instanceof xs&&this._menu._handleKeydown(r)})}return this._overlayRef}_getOverlayConfig(e){return new wn({positionStrategy:fo(this._injector,this._getOverlayOrigin()).withLockedPosition().withGrowAfterOpen().withTransformOriginOn(".mat-menu-panel, .mat-mdc-menu-panel"),backdropClass:e.backdropClass||"cdk-overlay-transparent-backdrop",panelClass:e.overlayPanelClass,scrollStrategy:this._scrollStrategy(),direction:this._dir||"ltr",disableAnimations:this._animationsDisabled})}_subscribeToPositions(e,i){e.setPositionClasses&&i.positionChanges.subscribe(r=>{this._ngZone.run(()=>{let o=r.connectionPair.overlayX==="start"?"after":"before",s=r.connectionPair.overlayY==="top"?"below":"above";e.setPositionClasses(o,s)})})}_setPosition(e,i){let[r,o]=e.xPosition==="before"?["end","start"]:["start","end"],[s,a]=e.yPosition==="above"?["bottom","top"]:["top","bottom"],[l,c]=[s,a],[u,f]=[r,o],h=0;if(this._triggersSubmenu()){if(f=r=e.xPosition==="before"?"start":"end",o=u=r==="end"?"start":"end",this._parentMaterialMenu){if(this._parentInnerPadding==null){let p=this._parentMaterialMenu.items.first;this._parentInnerPadding=p?p._getHostElement().offsetTop:0}h=s==="bottom"?this._parentInnerPadding:-this._parentInnerPadding}}else e.overlapTrigger||(l=s==="top"?"bottom":"top",c=a==="top"?"bottom":"top");i.withPositions([{originX:r,originY:l,overlayX:u,overlayY:s,offsetY:h},{originX:o,originY:l,overlayX:f,overlayY:s,offsetY:h},{originX:r,originY:c,overlayX:u,overlayY:a,offsetY:-h},{originX:o,originY:c,overlayX:f,overlayY:a,offsetY:-h}])}_menuClosingActions(){let e=this._getOutsideClickStream(this._overlayRef),i=this._overlayRef.detachments(),r=this._parentMaterialMenu?this._parentMaterialMenu.closed:Y(),o=this._parentMaterialMenu?this._parentMaterialMenu._hovered().pipe(ge(s=>this._menuOpen&&s!==this._menuItemInstance)):Y();return Nt(e,r,o,i)}_getPortal(e){return(!this._portal||this._portal.templateRef!==e.templateRef)&&(this._portal=new Dn(e.templateRef,this._viewContainerRef)),this._portal}_ownsMenu(e){return ws.get(e)===this}_triggerIsAriaDisabled(){return P(this._element.nativeElement.getAttribute("aria-disabled"))}static \u0275fac=function(i){Hd()};static \u0275dir=H({type:t})}return t})(),PE=(()=>{class t extends FF{_cleanupTouchstart;_hoverSubscription=pe.EMPTY;get _deprecatedMatMenuTriggerFor(){return this.menu}set _deprecatedMatMenuTriggerFor(e){this.menu=e}get menu(){return this._menu}set menu(e){this._menu=e}menuData;restoreFocus=!0;menuOpened=new W;onMenuOpen=this.menuOpened;menuClosed=new W;onMenuClose=this.menuClosed;constructor(){super(!0);let e=d(Re);this._cleanupTouchstart=e.listen(this._element.nativeElement,"touchstart",i=>{oo(i)||(this._openedBy="touch")},{passive:!0})}triggersSubmenu(){return super._triggersSubmenu()}toggleMenu(){return this.menuOpen?this.closeMenu():this.openMenu()}openMenu(){this._openMenu(!0)}closeMenu(){this._closeMenu()}updatePosition(){this._overlayRef?.updatePosition()}ngAfterContentInit(){this._handleHover()}ngOnDestroy(){super.ngOnDestroy(),this._cleanupTouchstart(),this._hoverSubscription.unsubscribe()}_getOverlayOrigin(){return this._element}_getOutsideClickStream(e){return e.backdropClick()}_handleMousedown(e){ro(e)||(this._openedBy=e.button===0?"mouse":void 0,this.triggersSubmenu()&&e.preventDefault())}_handleKeydown(e){let i=e.keyCode;(i===13||i===32)&&(this._openedBy="keyboard"),this.triggersSubmenu()&&(i===39&&this.dir==="ltr"||i===37&&this.dir==="rtl")&&(this._openedBy="keyboard",this.openMenu())}_handleClick(e){this.triggersSubmenu()?(e.stopPropagation(),this.openMenu()):this.toggleMenu()}_handleHover(){this.triggersSubmenu()&&this._parentMaterialMenu&&(this._hoverSubscription=this._parentMaterialMenu._hovered().subscribe(e=>{e===this._menuItemInstance&&!e.disabled&&this._parentMaterialMenu?._panelAnimationState!=="void"&&(this._openedBy="mouse",this._openMenu(!1))}))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,selectors:[["","mat-menu-trigger-for",""],["","matMenuTriggerFor",""]],hostAttrs:[1,"mat-mdc-menu-trigger"],hostVars:3,hostBindings:function(i,r){i&1&&D("click",function(s){return r._handleClick(s)})("mousedown",function(s){return r._handleMousedown(s)})("keydown",function(s){return r._handleKeydown(s)}),i&2&&re("aria-haspopup",r.menu?"menu":null)("aria-expanded",r.menuOpen)("aria-controls",r.menuOpen?r.menu?.panelId:null)},inputs:{_deprecatedMatMenuTriggerFor:[0,"mat-menu-trigger-for","_deprecatedMatMenuTriggerFor"],menu:[0,"matMenuTriggerFor","menu"],menuData:[0,"matMenuTriggerData","menuData"],restoreFocus:[0,"matMenuTriggerRestoreFocus","restoreFocus"]},outputs:{menuOpened:"menuOpened",onMenuOpen:"onMenuOpen",menuClosed:"menuClosed",onMenuClose:"onMenuClose"},exportAs:["matMenuTrigger"],features:[Me]})}return t})();var FE=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({imports:[ar,on,ve,_i]})}return t})();var _o=class{_multiple;_emitChanges;compareWith;_selection=new Set;_deselectedToEmit=[];_selectedToEmit=[];_selected=null;get selected(){return this._selected||(this._selected=Array.from(this._selection.values())),this._selected}changed=new E;bulk={select:n=>this._select(n),deselect:n=>this._deselect(n),setSelection:n=>this._setSelection(n)};constructor(n=!1,e,i=!0,r){this._multiple=n,this._emitChanges=i,this.compareWith=r,e&&e.length&&(n?e.forEach(o=>this._markSelected(o)):this._markSelected(e[0]),this._selectedToEmit.length=0)}select(...n){return this._select(n)}deselect(...n){return this._deselect(n)}setSelection(...n){return this._setSelection(n)}toggle(n){return this.isSelected(n)?this.deselect(n):this.select(n)}clear(n=!0){this._unmarkAll();let e=this._hasQueuedChanges();return n&&this._emitChangeEvent(),e}isSelected(n){return this._selection.has(this._getConcreteValue(n))}isEmpty(){return this._selection.size===0}hasValue(){return!this.isEmpty()}sort(n){this._multiple&&this.selected&&this._selected.sort(n)}isMultipleSelection(){return this._multiple}_select(n){this._verifyValueAssignment(n),n.forEach(i=>this._markSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}_deselect(n){this._verifyValueAssignment(n),n.forEach(i=>this._unmarkSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}_setSelection(n){this._verifyValueAssignment(n);let e=this.selected,i=new Set(n.map(o=>this._getConcreteValue(o)));n.forEach(o=>this._markSelected(o)),e.filter(o=>!i.has(this._getConcreteValue(o,i))).forEach(o=>this._unmarkSelected(o));let r=this._hasQueuedChanges();return this._emitChangeEvent(),r}_emitChangeEvent(){this._selected=null,(this._selectedToEmit.length||this._deselectedToEmit.length)&&(this.changed.next({source:this,added:this._selectedToEmit,removed:this._deselectedToEmit}),this._deselectedToEmit=[],this._selectedToEmit=[])}_markSelected(n){n=this._getConcreteValue(n),this.isSelected(n)||(this._multiple||this._unmarkAll(),this.isSelected(n)||this._selection.add(n),this._emitChanges&&this._selectedToEmit.push(n))}_unmarkSelected(n){n=this._getConcreteValue(n),this.isSelected(n)&&(this._selection.delete(n),this._emitChanges&&this._deselectedToEmit.push(n))}_unmarkAll(){this.isEmpty()||this._selection.forEach(n=>this._unmarkSelected(n))}_verifyValueAssignment(n){n.length>1&&this._multiple}_hasQueuedChanges(){return!!(this._deselectedToEmit.length||this._selectedToEmit.length)}_getConcreteValue(n,e){if(this.compareWith){e=e??this._selection;for(let i of e)if(this.compareWith(n,i))return i;return n}else return n}};var zE=(()=>{class t{_renderer;_elementRef;onChange=e=>{};onTouched=()=>{};constructor(e,i){this._renderer=e,this._elementRef=i}setProperty(e,i){this._renderer.setProperty(this._elementRef.nativeElement,e,i)}registerOnTouched(e){this.onTouched=e}registerOnChange(e){this.onChange=e}setDisabledState(e){this.setProperty("disabled",e)}static \u0275fac=function(i){return new(i||t)(ft(Re),ft(O))};static \u0275dir=H({type:t})}return t})(),VF=(()=>{class t extends zE{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ht(t)))(r||t)}})();static \u0275dir=H({type:t,features:[Me]})}return t})(),Ll=new b("");var BF={provide:Ll,useExisting:Pt(()=>$E),multi:!0};function jF(){let t=en()?en().getUserAgent():"";return/android (\d+)/.test(t.toLowerCase())}var HF=new b(""),$E=(()=>{class t extends zE{_compositionMode;_composing=!1;constructor(e,i,r){super(e,i),this._compositionMode=r,this._compositionMode==null&&(this._compositionMode=!jF())}writeValue(e){let i=e??"";this.setProperty("value",i)}_handleInput(e){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(e)}_compositionStart(){this._composing=!0}_compositionEnd(e){this._composing=!1,this._compositionMode&&this.onChange(e)}static \u0275fac=function(i){return new(i||t)(ft(Re),ft(O),ft(HF,8))};static \u0275dir=H({type:t,selectors:[["input","formControlName","",3,"type","checkbox",3,"ngNoCva",""],["textarea","formControlName","",3,"ngNoCva",""],["input","formControl","",3,"type","checkbox",3,"ngNoCva",""],["textarea","formControl","",3,"ngNoCva",""],["input","ngModel","",3,"type","checkbox",3,"ngNoCva",""],["textarea","ngModel","",3,"ngNoCva",""],["","ngDefaultControl",""]],hostBindings:function(i,r){i&1&&D("input",function(s){return r._handleInput(s.target.value)})("blur",function(){return r.onTouched()})("compositionstart",function(){return r._compositionStart()})("compositionend",function(s){return r._compositionEnd(s.target.value)})},standalone:!1,features:[Be([BF]),Me]})}return t})();function xv(t){return t==null||Iv(t)===0}function Iv(t){return t==null?null:Array.isArray(t)||typeof t=="string"?t.length:t instanceof Set?t.size:null}var Vl=new b(""),GE=new b(""),UF=/^(?=.{1,254}$)(?=.{1,64}@)[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/,yo=class{static min(n){return zF(n)}static max(n){return $F(n)}static required(n){return WE(n)}static requiredTrue(n){return GF(n)}static email(n){return WF(n)}static minLength(n){return qF(n)}static maxLength(n){return YF(n)}static pattern(n){return ZF(n)}static nullValidator(n){return ff()}static compose(n){return QE(n)}static composeAsync(n){return JE(n)}};function zF(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e<t?{min:{min:t,actual:n.value}}:null}}function $F(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e>t?{max:{max:t,actual:n.value}}:null}}function WE(t){return xv(t.value)?{required:!0}:null}function GF(t){return t.value===!0?null:{required:!0}}function WF(t){return xv(t.value)||UF.test(t.value)?null:{email:!0}}function qF(t){return n=>{let e=n.value?.length??Iv(n.value);return e===null||e===0?null:e<t?{minlength:{requiredLength:t,actualLength:e}}:null}}function YF(t){return n=>{let e=n.value?.length??Iv(n.value);return e!==null&&e>t?{maxlength:{requiredLength:t,actualLength:e}}:null}}function ZF(t){if(!t)return ff;let n,e;return typeof t=="string"?(e="",t.charAt(0)!=="^"&&(e+="^"),e+=t,t.charAt(t.length-1)!=="$"&&(e+="$"),n=new RegExp(e)):(e=t.toString(),n=t),i=>{if(xv(i.value))return null;let r=i.value;return n.test(r)?null:{pattern:{requiredPattern:e,actualValue:r}}}}function ff(t){return null}function qE(t){return t!=null}function YE(t){return di(t)?Ve(t):t}function ZE(t){let n={};return t.forEach(e=>{n=e!=null?v(v({},n),e):n}),Object.keys(n).length===0?null:n}function XE(t,n){return n.map(e=>e(t))}function XF(t){return!t.validate}function KE(t){return t.map(n=>XF(n)?n:e=>n.validate(e))}function QE(t){if(!t)return null;let n=t.filter(qE);return n.length==0?null:function(e){return ZE(XE(e,n))}}function Mv(t){return t!=null?QE(KE(t)):null}function JE(t){if(!t)return null;let n=t.filter(qE);return n.length==0?null:function(e){let i=XE(e,n).map(YE);return Zs(i).pipe(oe(ZE))}}function Tv(t){return t!=null?JE(KE(t)):null}function LE(t,n){return t===null?[n]:Array.isArray(t)?[...t,n]:[t,n]}function ew(t){return t._rawValidators}function tw(t){return t._rawAsyncValidators}function Ev(t){return t?Array.isArray(t)?t:[t]:[]}function hf(t,n){return Array.isArray(t)?t.includes(n):t===n}function VE(t,n){let e=Ev(n);return Ev(t).forEach(r=>{hf(e,r)||e.push(r)}),e}function BE(t,n){return Ev(n).filter(e=>!hf(t,e))}var mf=class{get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators=[];_rawAsyncValidators=[];_setValidators(n){this._rawValidators=n||[],this._composedValidatorFn=Mv(this._rawValidators)}_setAsyncValidators(n){this._rawAsyncValidators=n||[],this._composedAsyncValidatorFn=Tv(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_onDestroyCallbacks=[];_registerOnDestroy(n){this._onDestroyCallbacks.push(n)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(n=>n()),this._onDestroyCallbacks=[]}reset(n=void 0){this.control?.reset(n)}hasError(n,e){return this.control?this.control.hasError(n,e):!1}getError(n,e){return this.control?this.control.getError(n,e):null}},Ts=class extends mf{name;get formDirective(){return null}get path(){return null}};var Al="VALID",uf="INVALID",Is="PENDING",Rl="DISABLED",fr=class{},pf=class extends fr{value;source;constructor(n,e){super(),this.value=n,this.source=e}},Pl=class extends fr{pristine;source;constructor(n,e){super(),this.pristine=n,this.source=e}},Fl=class extends fr{touched;source;constructor(n,e){super(),this.touched=n,this.source=e}},Ms=class extends fr{status;source;constructor(n,e){super(),this.status=n,this.source=e}},gf=class extends fr{source;constructor(n){super(),this.source=n}},Ns=class extends fr{source;constructor(n){super(),this.source=n}};function nw(t){return(Sf(t)?t.validators:t)||null}function KF(t){return Array.isArray(t)?Mv(t):t||null}function iw(t,n){return(Sf(n)?n.asyncValidators:t)||null}function QF(t){return Array.isArray(t)?Tv(t):t||null}function Sf(t){return t!=null&&!Array.isArray(t)&&typeof t=="object"}function JF(t,n,e){let i=t.controls;if(!(n?Object.keys(i):i).length)throw new x(1e3,"");if(!rw(i,e))throw new x(1001,"")}function e1(t,n,e){t._forEachChild((i,r)=>{if(e[r]===void 0)throw new x(-1002,"")})}var vf=class{_pendingDirty=!1;_hasOwnPendingAsyncValidator=null;_pendingTouched=!1;_onCollectionChange=()=>{};_updateOn;_hasRequired=j(!1);_parent=null;_asyncValidationSubscription;_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators;_rawAsyncValidators;value;constructor(n,e){this._assignValidators(n),this._assignAsyncValidators(e)}get validator(){return this._composedValidatorFn}set validator(n){this._rawValidators=this._composedValidatorFn=n,this._updateHasRequiredValidator()}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(n){this._rawAsyncValidators=this._composedAsyncValidatorFn=n}get parent(){return this._parent}get status(){return Ne(this.statusReactive)}set status(n){Ne(()=>this.statusReactive.set(n))}_status=Q(()=>this.statusReactive());statusReactive=j(void 0);get valid(){return this.status===Al}get invalid(){return this.status===uf}get pending(){return this.status===Is}get disabled(){return this.status===Rl}get enabled(){return this.status!==Rl}errors;get pristine(){return Ne(this.pristineReactive)}set pristine(n){Ne(()=>this.pristineReactive.set(n))}_pristine=Q(()=>this.pristineReactive());pristineReactive=j(!0);get dirty(){return!this.pristine}get touched(){return Ne(this.touchedReactive)}set touched(n){Ne(()=>this.touchedReactive.set(n))}_touched=Q(()=>this.touchedReactive());touchedReactive=j(!1);get untouched(){return!this.touched}_events=new E;events=this._events.asObservable();valueChanges;statusChanges;get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(n){this._assignValidators(n)}setAsyncValidators(n){this._assignAsyncValidators(n)}addValidators(n){this.setValidators(VE(n,this._rawValidators))}addAsyncValidators(n){this.setAsyncValidators(VE(n,this._rawAsyncValidators))}removeValidators(n){this.setValidators(BE(n,this._rawValidators))}removeAsyncValidators(n){this.setAsyncValidators(BE(n,this._rawAsyncValidators))}hasValidator(n){return hf(this._rawValidators,n)}hasAsyncValidator(n){return hf(this._rawAsyncValidators,n)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(n={}){let e=this.touched===!1;this.touched=!0;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsTouched(k(v({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new Fl(!0,i))}markAllAsDirty(n={}){this.markAsDirty({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsDirty(n))}markAllAsTouched(n={}){this.markAsTouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsTouched(n))}markAsUntouched(n={}){let e=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsUntouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:i})}),n.onlySelf||this._parent?._updateTouched(n,i),e&&n.emitEvent!==!1&&this._events.next(new Fl(!1,i))}markAsDirty(n={}){let e=this.pristine===!0;this.pristine=!1;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsDirty(k(v({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new Pl(!1,i))}markAsPristine(n={}){let e=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsPristine({onlySelf:!0,emitEvent:n.emitEvent})}),n.onlySelf||this._parent?._updatePristine(n,i),e&&n.emitEvent!==!1&&this._events.next(new Pl(!0,i))}markAsPending(n={}){this.status=Is;let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Ms(this.status,e)),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.markAsPending(k(v({},n),{sourceControl:e}))}disable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=Rl,this.errors=null,this._forEachChild(r=>{r.disable(k(v({},n),{onlySelf:!0}))}),this._updateValue();let i=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new pf(this.value,i)),this._events.next(new Ms(this.status,i)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(k(v({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(r=>r(!0))}enable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=Al,this._forEachChild(i=>{i.enable(k(v({},n),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent}),this._updateAncestors(k(v({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(i=>i(!1))}_updateAncestors(n,e){n.onlySelf||(this._parent?.updateValueAndValidity(n),n.skipPristineCheck||this._parent?._updatePristine({},e),this._parent?._updateTouched({},e))}setParent(n){this._parent=n}getRawValue(){return this.value}updateValueAndValidity(n={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let i=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===Al||this.status===Is)&&this._runAsyncValidator(i,n.emitEvent)}let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new pf(this.value,e)),this._events.next(new Ms(this.status,e)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.updateValueAndValidity(k(v({},n),{sourceControl:e}))}_updateTreeValidity(n={emitEvent:!0}){this._forEachChild(e=>e._updateTreeValidity(n)),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?Rl:Al}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(n,e){if(this.asyncValidator){this.status=Is,this._hasOwnPendingAsyncValidator={emitEvent:e!==!1,shouldHaveEmitted:n!==!1};let i=YE(this.asyncValidator(this));this._asyncValidationSubscription=i.subscribe(r=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(r,{emitEvent:e,shouldHaveEmitted:n})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let n=(this._hasOwnPendingAsyncValidator?.emitEvent||this._hasOwnPendingAsyncValidator?.shouldHaveEmitted)??!1;return this._hasOwnPendingAsyncValidator=null,n}return!1}setErrors(n,e={}){this.errors=n,this._updateControlsErrors(e.emitEvent!==!1,this,e.shouldHaveEmitted)}get(n){let e=n;return e==null||(Array.isArray(e)||(e=e.split(".")),e.length===0)?null:e.reduce((i,r)=>i&&i._find(r),this)}getError(n,e){let i=e?this.get(e):this;return i?.errors?i.errors[n]:null}hasError(n,e){return!!this.getError(n,e)}get root(){let n=this;for(;n._parent;)n=n._parent;return n}_updateControlsErrors(n,e,i){this.status=this._calculateStatus(),n&&this.statusChanges.emit(this.status),(n||i)&&this._events.next(new Ms(this.status,e)),this._parent&&this._parent._updateControlsErrors(n,e,i)}_initObservables(){this.valueChanges=new W,this.statusChanges=new W}_calculateStatus(){return this._allControlsDisabled()?Rl:this.errors?uf:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(Is)?Is:this._anyControlsHaveStatus(uf)?uf:Al}_anyControlsHaveStatus(n){return this._anyControls(e=>e.status===n)}_anyControlsDirty(){return this._anyControls(n=>n.dirty)}_anyControlsTouched(){return this._anyControls(n=>n.touched)}_updatePristine(n,e){let i=!this._anyControlsDirty(),r=this.pristine!==i;this.pristine=i,n.onlySelf||this._parent?._updatePristine(n,e),r&&this._events.next(new Pl(this.pristine,e))}_updateTouched(n={},e){this.touched=this._anyControlsTouched(),this._events.next(new Fl(this.touched,e)),n.onlySelf||this._parent?._updateTouched(n,e)}_onDisabledChange=[];_registerOnCollectionChange(n){this._onCollectionChange=n}_setUpdateStrategy(n){Sf(n)&&n.updateOn!=null&&(this._updateOn=n.updateOn)}_parentMarkedDirty(n){return!n&&!!this._parent?.dirty&&!this._parent._anyControlsDirty()}_find(n){return null}_assignValidators(n){this._rawValidators=Array.isArray(n)?n.slice():n,this._composedValidatorFn=KF(this._rawValidators),this._updateHasRequiredValidator()}_assignAsyncValidators(n){this._rawAsyncValidators=Array.isArray(n)?n.slice():n,this._composedAsyncValidatorFn=QF(this._rawAsyncValidators)}_updateHasRequiredValidator(){Ne(()=>this._hasRequired.set(this.hasValidator(yo.required)))}};function rw(t,n){return Object.hasOwn(t,n)}function t1(t){return t.tagName==="INPUT"||t.tagName==="SELECT"||t.tagName==="TEXTAREA"}function n1(t,n,e,i){switch(e){case"name":t.setAttribute(n,e,i);break;case"disabled":case"readonly":case"required":i?t.setAttribute(n,e,""):t.removeAttribute(n,e);break;case"max":case"min":case"minLength":case"maxLength":i!==void 0?t.setAttribute(n,e,i.toString()):t.removeAttribute(n,e);break}}var wv=class{kind;context;control;message;constructor({kind:n,context:e,control:i}){this.kind=n,this.context=e,this.control=i}};var i1=(()=>{class t{_validator=ff;_onChange;_enabled;ngOnChanges(e){if(this.inputName in e){let i=this.normalizeInput(e[this.inputName].currentValue);this._enabled=this.enabled(i),this._validator=this._enabled?this.createValidator(i):ff,this._onChange?.()}}validate(e){return this._validator(e)}registerOnValidatorChange(e){this._onChange=e}enabled(e){return e!=null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,features:[at]})}return t})();var r1={provide:Vl,useExisting:Pt(()=>ow),multi:!0};var ow=(()=>{class t extends i1{required;inputName="required";normalizeInput=P;createValidator=e=>WE;enabled(e){return e}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ht(t)))(r||t)}})();static \u0275dir=H({type:t,selectors:[["","required","","formControlName","",3,"type","checkbox"],["","required","","formControl","",3,"type","checkbox"],["","required","","ngModel","",3,"type","checkbox"]],hostVars:1,hostBindings:function(i,r){i&2&&re("required",r._enabled?"":null)},inputs:{required:"required"},standalone:!1,features:[Be([r1]),Me]})}return t})();var sw=new b("",{factory:()=>o1}),o1="always";function jE(t,n,e=!0){let i=()=>{};n?.valueAccessor?.registerOnChange(i),n?.valueAccessor?.registerOnTouched(i),_f(t,n),t&&(n._invokeOnDestroyCallbacks(),t._registerOnCollectionChange(()=>{}))}function bf(t,n){t.forEach(e=>{e.registerOnValidatorChange&&e.registerOnValidatorChange(n)})}function aw(t,n){let e=ew(t);n.validator!==null?t.setValidators(LE(e,n.validator)):typeof e=="function"&&t.setValidators([e]);let i=tw(t);n.asyncValidator!==null?t.setAsyncValidators(LE(i,n.asyncValidator)):typeof i=="function"&&t.setAsyncValidators([i]);let r=()=>t.updateValueAndValidity();bf(n._rawValidators,r),bf(n._rawAsyncValidators,r)}function _f(t,n){let e=!1;if(t!==null){if(n.validator!==null){let r=ew(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(s=>s!==n.validator);o.length!==r.length&&(e=!0,t.setValidators(o))}}if(n.asyncValidator!==null){let r=tw(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(s=>s!==n.asyncValidator);o.length!==r.length&&(e=!0,t.setAsyncValidators(o))}}}let i=()=>{};return bf(n._rawValidators,i),bf(n._rawAsyncValidators,i),e}function lw(t,n){t==null,aw(t,n)}function s1(t,n){return _f(t,n)}function a1(t){return Object.getPrototypeOf(t.constructor)===VF}function cw(t,n){t._syncPendingControls(),n.forEach(e=>{let i=e.control;i.updateOn==="submit"&&i._pendingChange&&(e.viewToModelUpdate(i._pendingValue),i._pendingChange=!1)})}function l1(t,n){if(!n)return null;Array.isArray(n);let e,i,r;return n.forEach(o=>{o.constructor===$E?e=o:a1(o)?i=o:r=o}),r||i||e||null}function c1(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}var ks=class extends mf{_parent=null;name=null;valueAccessor=null;isCustomControlBased=!1;userOnReset;resetSubscription;set onReset(n){this.userOnReset=n,this.resetSubscription?.unsubscribe(),this.resetSubscription=void 0,this.control&&(this.resetSubscription=this.control.events.subscribe(e=>{e instanceof Ns&&this.control&&this.userOnReset?.(this.control.value)}),this.subscription?.add(this.resetSubscription))}isNativeFormElement=!1;rawValueAccessors;_selectedValueAccessor=null;get selectedValueAccessor(){return this._selectedValueAccessor??=l1(this,this.rawValueAccessors)}parseErrorsValidator=null;renderer;injector;requiredValidatorViaDi;subscription;customControlBindings=null;constructor(n,e,i){super(),this.injector=n,this.renderer=e,this.rawValueAccessors=i,this.injector?.get(We)?.onDestroy(()=>{this.removeParseErrorsValidator(this.control),this.subscription?.unsubscribe()})}setupCustomControl(){this.subscription?.unsubscribe();let n=this.injector?.get(Oe);if(!this.control||!n)return;let e=n.markForCheck.bind(n);this.subscription=new pe,this.subscription.add(this.control.valueChanges.subscribe(e)),this.subscription.add(this.control.statusChanges.subscribe(e)),this.resetSubscription?.unsubscribe(),this.resetSubscription=void 0,this.userOnReset&&(this.resetSubscription=this.control.events.subscribe(i=>{i instanceof Ns&&this.control&&this.userOnReset?.(this.control.value)}),this.subscription.add(this.resetSubscription)),this.parseErrorsValidator&&this.control.addValidators(this.parseErrorsValidator)}ngControlCreate(n){!n.nativeElement.hasAttribute?.("ngNoCva")&&(this.rawValueAccessors&&this.rawValueAccessors.length>0||this.valueAccessor!==null)||!n.customControl||(this.isCustomControlBased=!0,n.listenToCustomControlModel(r=>{this.control?.setValue(r,{emitModelToViewChange:!1}),this.control?.markAsDirty(),this.viewToModelUpdate(r)}),n.listenToCustomControlOutput("touch",()=>{this.control?.markAsTouched()}),this.customControlBindings={},this.isNativeFormElement=t1(n.nativeElement),this.requiredValidatorViaDi=this._rawValidators.find(r=>r instanceof ow))}ngControlUpdate(n,e){if(!this.isCustomControlBased)return;let i=this.control,r=this.customControlBindings;Object.is(r.value,i.value)||(r.value=i.value,n.setCustomControlModelInput(i.value)),this.bindControlProperty(n,r,"touched",i.touched),this.bindControlProperty(n,r,"dirty",i.dirty),this.bindControlProperty(n,r,"valid",i.valid),this.bindControlProperty(n,r,"invalid",i.invalid),this.bindControlProperty(n,r,"pending",i.pending),this.bindControlProperty(n,r,"disabled",i.disabled),this.shouldBindRequired&&this.bindControlProperty(n,r,"required",this.isRequired);let o=i.errors;if(r.errors!==o){r.errors=o;let s=this._convertErrors(o);n.setInputOnDirectives("errors",s)}}get isRequired(){return(this.requiredValidatorViaDi?._enabled||this.control?._hasRequired())??!1}get shouldBindRequired(){return!0}bindControlProperty(n,e,i,r){if(e[i]===r)return;e[i]=r;let o=n.setInputOnDirectives(i,r);this.isNativeFormElement&&!o&&(i==="disabled"||i==="required")&&this.renderer&&n1(this.renderer,n.nativeElement,i,r)}_convertErrors(n){if(n===null)return[];let e=this.control;return Object.entries(n).map(([i,r])=>new wv({context:r,kind:i,control:e}))}setParseErrorSource(n){if(n===void 0)return;let e=null,i=Q(()=>{let r=n();return r.length===0?null:r.reduce((o,s)=>(o[s.kind]=s,o),{})});this.parseErrorsValidator=(()=>e).bind(this),ut(()=>{e=i(),this.control?.updateValueAndValidity({emitEvent:!1})},{injector:this.injector})}removeParseErrorsValidator(n){this.parseErrorsValidator&&(n?.removeValidators(this.parseErrorsValidator),n?.updateValueAndValidity({emitEvent:!1}))}};var yf=class extends vf{constructor(n,e,i){super(nw(e),iw(i,e)),this.controls=n,this._initObservables(),this._setUpdateStrategy(e),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;registerControl(n,e){let i=this._find(n);return i||(this.controls[n]=e,e.setParent(this),e._registerOnCollectionChange(this._onCollectionChange),e)}addControl(n,e,i={}){this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}removeControl(n,e={}){let i=this._find(n);i&&i._registerOnCollectionChange(()=>{}),delete this.controls[n],this.updateValueAndValidity({emitEvent:e.emitEvent}),this._onCollectionChange()}setControl(n,e,i={}){let r=this._find(n);r&&r._registerOnCollectionChange(()=>{}),delete this.controls[n],e&&this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}contains(n){return this._find(n)?.enabled===!0}setValue(n,e={}){Ne(()=>{e1(this,!0,n),Object.keys(n).forEach(i=>{JF(this,!0,i),this.controls[i].setValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e)})}patchValue(n,e={}){n!=null&&(Object.keys(n).forEach(i=>{let r=this._find(i);r&&r.patchValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e))}reset(n={},e={}){this._forEachChild((i,r)=>{i.reset(n?n[r]:null,k(v({},e),{onlySelf:!0}))}),this._updatePristine(e,this),this._updateTouched(e,this),this.updateValueAndValidity(e),e?.emitEvent!==!1&&this._events.next(new Ns(this))}getRawValue(){return this._reduceChildren({},(n,e,i)=>(n[i]=e.getRawValue(),n))}_syncPendingControls(){let n=this._reduceChildren(!1,(e,i)=>i._syncPendingControls()?!0:e);return n&&this.updateValueAndValidity({onlySelf:!0}),n}_forEachChild(n){Object.keys(this.controls).forEach(e=>{let i=this.controls[e];i&&n(i,e)})}_setUpControls(){this._forEachChild(n=>{n.setParent(this),n._registerOnCollectionChange(this._onCollectionChange)})}_updateValue(){this.value=this._reduceValue()}_anyControls(n){for(let[e,i]of Object.entries(this.controls))if(this.contains(e)&&n(i))return!0;return!1}_reduceValue(){let n={};return this._reduceChildren(n,(e,i,r)=>((i.enabled||this.disabled)&&(e[r]=i.value),e))}_reduceChildren(n,e){let i=n;return this._forEachChild((r,o)=>{i=e(i,r,o)}),i}_allControlsDisabled(){for(let n of Object.keys(this.controls))if(this.controls[n].enabled)return!1;return Object.keys(this.controls).length>0||this.disabled}_find(n){return rw(this.controls,n)?this.controls[n]:null}};var d1={provide:Ts,useExisting:Pt(()=>Bl)},Ol=Promise.resolve(),Bl=(()=>{class t extends Ts{callSetDisabledState;get submitted(){return Ne(this.submittedReactive)}_submitted=Q(()=>this.submittedReactive());submittedReactive=j(!1);_directives=new Set;form;ngSubmit=new W;options;constructor(e,i,r){super(),this.callSetDisabledState=r,this.form=new yf({},Mv(e),Tv(i))}ngAfterViewInit(){this._setUpdateStrategy()}get formDirective(){return this}get control(){return this.form}get path(){return[]}get controls(){return this.form.controls}addControl(e){Ol.then(()=>{let i=this._findContainer(e.path);e.control=i.registerControl(e.name,e.control),e._setupWithForm(this.callSetDisabledState),e.control.updateValueAndValidity({emitEvent:!1}),this._directives.add(e)})}getControl(e){return this.form.get(e.path)}removeControl(e){Ol.then(()=>{this._findContainer(e.path)?.removeControl(e.name),this._directives.delete(e)})}addFormGroup(e){Ol.then(()=>{let i=this._findContainer(e.path),r=new yf({});lw(r,e),i.registerControl(e.name,r),r.updateValueAndValidity({emitEvent:!1})})}removeFormGroup(e){Ol.then(()=>{this._findContainer(e.path)?.removeControl?.(e.name)})}getFormGroup(e){return this.form.get(e.path)}updateModel(e,i){Ol.then(()=>{this.form.get(e.path).setValue(i)})}setValue(e){this.control.setValue(e)}onSubmit(e){return this.submittedReactive.set(!0),cw(this.form,this._directives),this.ngSubmit.emit(e),this.form._events.next(new gf(this.control)),e?.target?.method==="dialog"}onReset(){this.resetForm()}resetForm(e=void 0){this.form.reset(e),this.submittedReactive.set(!1)}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.form._updateOn=this.options.updateOn)}_findContainer(e){return e.pop(),e.length?this.form.get(e):this.form}static \u0275fac=function(i){return new(i||t)(ft(Vl,10),ft(GE,10),ft(sw,8))};static \u0275dir=H({type:t,selectors:[["form",3,"ngNoForm","",3,"formGroup","",3,"formArray",""],["ng-form"],["","ngForm",""]],hostBindings:function(i,r){i&1&&D("submit",function(s){return r.onSubmit(s)})("reset",function(){return r.onReset()})},inputs:{options:[0,"ngFormOptions","options"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[Be([d1]),Me]})}return t})();function HE(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function UE(t){return typeof t=="object"&&t!==null&&Object.keys(t).length===2&&"value"in t&&"disabled"in t}var u1=class extends vf{defaultValue=null;_onChange=[];_pendingValue;_pendingChange=!1;constructor(n=null,e,i){super(nw(e),iw(i,e)),this._applyFormState(n),this._setUpdateStrategy(e),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),Sf(e)&&(e.nonNullable||e.initialValueIsDefault)&&(UE(n)?this.defaultValue=n.value:this.defaultValue=n)}setValue(n,e={}){Ne(()=>{this.value=this._pendingValue=n,this._onChange.length&&e.emitModelToViewChange!==!1&&this._onChange.forEach(i=>i(this.value,e.emitViewToModelChange!==!1)),this.updateValueAndValidity(e)})}patchValue(n,e={}){this.setValue(n,e)}reset(n=this.defaultValue,e={}){this._applyFormState(n),this.markAsPristine(e),this.markAsUntouched(e),this.setValue(this.value,e),e.overwriteDefaultValue&&(this.defaultValue=this.value),this._pendingChange=!1,e?.emitEvent!==!1&&this._events.next(new Ns(this))}_updateValue(){}_anyControls(n){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(n){this._onChange.push(n)}_unregisterOnChange(n){HE(this._onChange,n)}registerOnDisabledChange(n){this._onDisabledChange.push(n)}_unregisterOnDisabledChange(n){HE(this._onDisabledChange,n)}_forEachChild(n){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(n){UE(n)?(this.value=this._pendingValue=n.value,n.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=n}};var f1=t=>t instanceof u1;var h1=(()=>{class t extends Ts{callSetDisabledState;get submitted(){return Ne(this._submittedReactive)}set submitted(e){this._submittedReactive.set(e)}_submitted=Q(()=>this._submittedReactive());_submittedReactive=j(!1);_oldForm;_onCollectionChange=()=>this._updateDomValue();directives=[];constructor(e,i,r){super(),this.callSetDisabledState=r,this._setValidators(e),this._setAsyncValidators(i)}ngOnChanges(e){this.onChanges(e)}ngOnDestroy(){this.onDestroy()}onChanges(e){this._checkFormPresent(),Object.hasOwn(e,"form")&&(this._updateValidators(),this._updateDomValue(),this._updateRegistrations(),this._oldForm=this.form)}onDestroy(){this.form&&(_f(this.form,this),this.form._onCollectionChange===this._onCollectionChange&&this.form._registerOnCollectionChange(()=>{}))}get formDirective(){return this}get path(){return[]}addControl(e){let i=this.form.get(e.path);return e._setupWithForm(i,this.callSetDisabledState),i.updateValueAndValidity({emitEvent:!1}),this.directives.push(e),i}getControl(e){return this.form.get(e.path)}removeControl(e){jE(e.control||null,e,!1),c1(this.directives,e)}addFormGroup(e){this._setUpFormContainer(e)}removeFormGroup(e){this._cleanUpFormContainer(e)}getFormGroup(e){return this.form.get(e.path)}getFormArray(e){return this.form.get(e.path)}addFormArray(e){this._setUpFormContainer(e)}removeFormArray(e){this._cleanUpFormContainer(e)}updateModel(e,i){this.form.get(e.path).setValue(i)}onReset(){this.resetForm()}resetForm(e=void 0,i={}){this.form.reset(e,i),this._submittedReactive.set(!1)}onSubmit(e){return this.submitted=!0,cw(this.form,this.directives),this.ngSubmit.emit(e),this.form._events.next(new gf(this.control)),e?.target?.method==="dialog"}_updateDomValue(){this.directives.forEach(e=>{let i=e.control,r=this.form.get(e.path);i!==r&&(jE(i||null,e),f1(r)&&e._setupWithForm(r,this.callSetDisabledState))}),this.form._updateTreeValidity({emitEvent:!1})}_setUpFormContainer(e){let i=this.form.get(e.path);lw(i,e),i.updateValueAndValidity({emitEvent:!1})}_cleanUpFormContainer(e){let i=this.form?.get(e.path);i&&s1(i,e)&&i.updateValueAndValidity({emitEvent:!1})}_updateRegistrations(){this.form._registerOnCollectionChange(this._onCollectionChange),this._oldForm?._registerOnCollectionChange(()=>{})}_updateValidators(){aw(this.form,this),this._oldForm&&_f(this._oldForm,this)}_checkFormPresent(){this.form}static \u0275fac=function(i){return new(i||t)(ft(Vl,10),ft(GE,10),ft(sw,8))};static \u0275dir=H({type:t,features:[Me,at]})}return t})(),m1={provide:Ts,useExisting:Pt(()=>jl)},jl=(()=>{class t extends h1{form=null;ngSubmit=new W;get control(){return this.form}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ht(t)))(r||t)}})();static \u0275dir=H({type:t,selectors:[["","formGroup",""]],hostBindings:function(i,r){i&1&&D("submit",function(s){return r.onSubmit(s)})("reset",function(){return r.onReset()})},inputs:{form:[0,"formGroup","form"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[Be([m1]),Me]})}return t})();var Cf=new b("");var Df=(()=>{class t{_animationsDisabled=Ce();state="unchecked";disabled=!1;appearance="full";static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-pseudo-checkbox"]],hostAttrs:[1,"mat-pseudo-checkbox"],hostVars:12,hostBindings:function(i,r){i&2&&z("mat-pseudo-checkbox-indeterminate",r.state==="indeterminate")("mat-pseudo-checkbox-checked",r.state==="checked")("mat-pseudo-checkbox-disabled",r.disabled)("mat-pseudo-checkbox-minimal",r.appearance==="minimal")("mat-pseudo-checkbox-full",r.appearance==="full")("_mat-animation-noopable",r._animationsDisabled)},inputs:{state:"state",disabled:"disabled",appearance:"appearance"},decls:0,vars:0,template:function(i,r){},styles:[`.mat-pseudo-checkbox {
  border-radius: 2px;
  cursor: pointer;
  display: inline-block;
  vertical-align: middle;
  box-sizing: border-box;
  position: relative;
  flex-shrink: 0;
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 0.1), background-color 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox::after {
  position: absolute;
  opacity: 0;
  content: "";
  border-bottom: 2px solid currentColor;
  transition: opacity 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-pseudo-checkbox._mat-animation-noopable::after {
  transition: none;
}

.mat-pseudo-checkbox-disabled {
  cursor: default;
}

.mat-pseudo-checkbox-indeterminate::after {
  left: 1px;
  opacity: 1;
  border-radius: 2px;
}

.mat-pseudo-checkbox-checked::after {
  left: 1px;
  border-left: 2px solid currentColor;
  transform: rotate(-45deg);
  opacity: 1;
  box-sizing: content-box;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  color: var(--%NS%mat-pseudo-checkbox-minimal-selected-checkmark-color, var(--%NS%mat-sys-primary));
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--%NS%mat-pseudo-checkbox-minimal-disabled-selected-checkmark-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}

.mat-pseudo-checkbox-full {
  border-color: var(--%NS%mat-pseudo-checkbox-full-unselected-icon-color, var(--%NS%mat-sys-on-surface-variant));
  border-width: 2px;
  border-style: solid;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-disabled {
  border-color: var(--%NS%mat-pseudo-checkbox-full-disabled-unselected-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate {
  background-color: var(--%NS%mat-pseudo-checkbox-full-selected-icon-color, var(--%NS%mat-sys-primary));
  border-color: transparent;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  color: var(--%NS%mat-pseudo-checkbox-full-selected-checkmark-color, var(--%NS%mat-sys-on-primary));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled {
  background-color: var(--%NS%mat-pseudo-checkbox-full-disabled-selected-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--%NS%mat-pseudo-checkbox-full-disabled-selected-checkmark-color, var(--%NS%mat-sys-surface));
}

.mat-pseudo-checkbox {
  width: 18px;
  height: 18px;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after {
  width: 14px;
  height: 6px;
  transform-origin: center;
  top: -4.2426406871px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  top: 8px;
  width: 16px;
}

.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after {
  width: 10px;
  height: 4px;
  transform-origin: center;
  top: -2.8284271247px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  top: 6px;
  width: 12px;
}
`],encapsulation:2})}return t})();var p1=["text"],g1=[[["mat-icon"]],"*"],v1=["mat-icon","*"];function b1(t,n){if(t&1&&ie(0,"mat-pseudo-checkbox",1),t&2){let e=w();U("disabled",e.disabled)("state",e.selected?"checked":"unchecked")}}function _1(t,n){if(t&1&&ie(0,"mat-pseudo-checkbox",3),t&2){let e=w();U("disabled",e.disabled)}}function y1(t,n){if(t&1&&(m(0,"span",4),y(1),g()),t&2){let e=w();_(),At("(",e.group.label,")")}}var kv=new b("MAT_OPTION_PARENT_COMPONENT"),Av=new b("MatOptgroup");var Nv=class{source;isUserInput;constructor(n,e=!1){this.source=n,this.isUserInput=e}},hr=(()=>{class t{_element=d(O);_changeDetectorRef=d(Oe);_parent=d(kv,{optional:!0});group=d(Av,{optional:!0});_signalDisableRipple=!1;_selected=!1;_active=!1;_mostRecentViewValue="";get multiple(){return this._parent&&this._parent.multiple}get selected(){return this._selected}value;id=d(xe).getId("mat-option-");get disabled(){return this.group&&this.group.disabled||this._disabled()}set disabled(e){this._disabled.set(e)}_disabled=j(!1);get disableRipple(){return this._signalDisableRipple?this._parent.disableRipple():!!this._parent?.disableRipple}get hideSingleSelectionIndicator(){return!!(this._parent&&this._parent.hideSingleSelectionIndicator)}onSelectionChange=new W;_text;_stateChanges=new E;constructor(){let e=d(ze);e.load(qn),e.load(ms),this._signalDisableRipple=!!this._parent&&Zt(this._parent.disableRipple)}get active(){return this._active}get viewValue(){return(this._text?.nativeElement.textContent||"").trim()}select(e=!0){this._selected||(this._selected=!0,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}deselect(e=!0){this._selected&&(this._selected=!1,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}focus(e,i){let r=this._getHostElement();typeof r.focus=="function"&&r.focus(i)}setActiveStyles(){this._active||(this._active=!0,this._changeDetectorRef.markForCheck())}setInactiveStyles(){this._active&&(this._active=!1,this._changeDetectorRef.markForCheck())}getLabel(){return this.viewValue}_handleKeydown(e){(e.keyCode===13||e.keyCode===32)&&!$e(e)&&(this._selectViaInteraction(),e.preventDefault())}_selectViaInteraction(){this.disabled||(this._selected=this.multiple?!this._selected:!0,this._changeDetectorRef.markForCheck(),this._emitSelectionChangeEvent(!0))}_getTabIndex(){return this.disabled?"-1":"0"}_getHostElement(){return this._element.nativeElement}ngAfterViewChecked(){if(this._selected){let e=this.viewValue;e!==this._mostRecentViewValue&&(this._mostRecentViewValue&&this._stateChanges.next(),this._mostRecentViewValue=e)}}ngOnDestroy(){this._stateChanges.complete()}_emitSelectionChangeEvent(e=!1){this.onSelectionChange.emit(new Nv(this,e))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-option"]],viewQuery:function(i,r){if(i&1&&Ue(p1,7),i&2){let o;ce(o=de())&&(r._text=o.first)}},hostAttrs:["role","option",1,"mat-mdc-option","mdc-list-item"],hostVars:11,hostBindings:function(i,r){i&1&&D("click",function(){return r._selectViaInteraction()})("keydown",function(s){return r._handleKeydown(s)}),i&2&&(Et("id",r.id),re("aria-selected",r.selected)("aria-disabled",r.disabled.toString()),z("mdc-list-item--selected",r.selected)("mat-mdc-option-multiple",r.multiple)("mat-mdc-option-active",r.active)("mdc-list-item--disabled",r.disabled))},inputs:{value:"value",id:"id",disabled:[2,"disabled","disabled",P]},outputs:{onSelectionChange:"onSelectionChange"},exportAs:["matOption"],ngContentSelectors:v1,decls:8,vars:5,consts:[["text",""],["aria-hidden","true",1,"mat-mdc-option-pseudo-checkbox",3,"disabled","state"],[1,"mdc-list-item__primary-text"],["state","checked","aria-hidden","true","appearance","minimal",1,"mat-mdc-option-pseudo-checkbox",3,"disabled"],[1,"cdk-visually-hidden"],["aria-hidden","true","mat-ripple","",1,"mat-mdc-option-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled"]],template:function(i,r){i&1&&(Pe(g1),V(0,b1,1,2,"mat-pseudo-checkbox",1),he(1),m(2,"span",2,0),he(4,1),g(),V(5,_1,1,1,"mat-pseudo-checkbox",3),V(6,y1,2,1,"span",4),ie(7,"div",5)),i&2&&(B(r.multiple?0:-1),_(5),B(!r.multiple&&r.selected&&!r.hideSingleSelectionIndicator?5:-1),_(),B(r.group&&r.group._inert?6:-1),_(),U("matRippleTrigger",r._getHostElement())("matRippleDisabled",r.disabled||r.disableRipple))},dependencies:[Df,sr],styles:[`.mat-mdc-option {
  -webkit-user-select: none;
  user-select: none;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  display: flex;
  position: relative;
  align-items: center;
  justify-content: flex-start;
  overflow: hidden;
  min-height: 48px;
  padding: 0 16px;
  cursor: pointer;
  -webkit-tap-highlight-color: transparent;
  color: var(--%NS%mat-option-label-text-color, var(--%NS%mat-sys-on-surface));
  font-family: var(--%NS%mat-option-label-text-font, var(--%NS%mat-sys-label-large-font));
  line-height: var(--%NS%mat-option-label-text-line-height, var(--%NS%mat-sys-label-large-line-height));
  font-size: var(--%NS%mat-option-label-text-size, var(--%NS%mat-sys-body-large-size));
  letter-spacing: var(--%NS%mat-option-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
  font-weight: var(--%NS%mat-option-label-text-weight, var(--%NS%mat-sys-body-large-weight));
}
.mat-mdc-option:hover:not(.mdc-list-item--disabled) {
  background-color: var(--%NS%mat-option-hover-state-layer-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) calc(var(--%NS%mat-sys-hover-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-option:focus.mdc-list-item, .mat-mdc-option.mat-mdc-option-active.mdc-list-item {
  background-color: var(--%NS%mat-option-focus-state-layer-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) calc(var(--%NS%mat-sys-focus-state-layer-opacity) * 100%), transparent));
  outline: 0;
}
.mat-mdc-option.mdc-list-item--%NS%selected:not(.mdc-list-item--disabled):not(.mat-mdc-option-active, .mat-mdc-option-multiple, :focus, :hover) {
  background-color: var(--%NS%mat-option-selected-state-layer-color, var(--%NS%mat-sys-secondary-container));
}
.mat-mdc-option.mdc-list-item--%NS%selected:not(.mdc-list-item--disabled):not(.mat-mdc-option-active, .mat-mdc-option-multiple, :focus, :hover) .mdc-list-item__primary-text {
  color: var(--%NS%mat-option-selected-state-label-text-color, var(--%NS%mat-sys-on-secondary-container));
}
.mat-mdc-option .mat-pseudo-checkbox {
  --%NS%mat-pseudo-checkbox-minimal-selected-checkmark-color: var(--%NS%mat-option-selected-state-label-text-color, var(--%NS%mat-sys-on-secondary-container));
}
.mat-mdc-option.mdc-list-item {
  align-items: center;
  background: transparent;
}
.mat-mdc-option.mdc-list-item--disabled {
  cursor: default;
  pointer-events: none;
}
.mat-mdc-option.mdc-list-item--disabled .mat-mdc-option-pseudo-checkbox, .mat-mdc-option.mdc-list-item--disabled .mdc-list-item__primary-text, .mat-mdc-option.mdc-list-item--disabled > mat-icon {
  opacity: 0.38;
}
.mat-mdc-optgroup .mat-mdc-option:not(.mat-mdc-option-multiple) {
  padding-left: 32px;
}
[dir=rtl] .mat-mdc-optgroup .mat-mdc-option:not(.mat-mdc-option-multiple) {
  padding-left: 16px;
  padding-right: 32px;
}
.mat-mdc-option .mat-icon,
.mat-mdc-option .mat-pseudo-checkbox-full {
  margin-right: 16px;
  flex-shrink: 0;
}
[dir=rtl] .mat-mdc-option .mat-icon,
[dir=rtl] .mat-mdc-option .mat-pseudo-checkbox-full {
  margin-right: 0;
  margin-left: 16px;
}
.mat-mdc-option .mat-pseudo-checkbox-minimal {
  margin-left: 16px;
  flex-shrink: 0;
}
[dir=rtl] .mat-mdc-option .mat-pseudo-checkbox-minimal {
  margin-right: 16px;
  margin-left: 0;
}
.mat-mdc-option .mat-mdc-option-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}
.mat-mdc-option .mdc-list-item__primary-text {
  white-space: normal;
  font-size: inherit;
  font-weight: inherit;
  letter-spacing: inherit;
  line-height: inherit;
  font-family: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  margin-right: auto;
}
[dir=rtl] .mat-mdc-option .mdc-list-item__primary-text {
  margin-right: 0;
  margin-left: auto;
}
@media (forced-colors: active) {
  .mat-mdc-option.mdc-list-item--%NS%selected:not(:has(.mat-mdc-option-pseudo-checkbox))::after {
    content: "";
    position: absolute;
    top: 50%;
    right: 16px;
    transform: translateY(-50%);
    width: 10px;
    height: 0;
    border-bottom: solid 10px;
    border-radius: 10px;
  }
  [dir=rtl] .mat-mdc-option.mdc-list-item--%NS%selected:not(:has(.mat-mdc-option-pseudo-checkbox))::after {
    right: auto;
    left: 16px;
  }
}

.mat-mdc-option-multiple {
  --%NS%mat-list-list-item-selected-container-color: var(--%NS%mat-list-list-item-container-color, transparent);
}

.mat-mdc-option-active .mat-focus-indicator::before {
  content: "";
}
`],encapsulation:2})}return t})();function dw(t,n,e){if(e.length){let i=n.toArray(),r=e.toArray(),o=0;for(let s=0;s<t+1;s++)i[s].group&&i[s].group===r[o]&&o++;return o}return 0}function uw(t,n,e,i){return t<e?t:t+n>e+i?Math.max(0,t-i+n):e}var Ef=(()=>{class t{isErrorState(e,i){return!!(e&&e.invalid&&(e.touched||i&&i.submitted))}isSignalErrorState(e){if(!e)return!1;let i=e().invalid(),r=e().touched();return i&&r}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();var As=class{_defaultMatcher;_parentFormGroup;_parentForm;_stateChanges;errorState=!1;matcher;ngControl;formField;constructor(n,e,i,r,o){this._defaultMatcher=n,this._parentFormGroup=i,this._parentForm=r,this._stateChanges=o,e?Zt(e.field)&&!e.updateValueAndValidity?(this.formField=e,this.ngControl=null):(this.formField=null,this.ngControl=e):this.ngControl=this.formField=null}updateErrorState(){let n=this.errorState,e=this._getCurrentErrorState(this.matcher||this._defaultMatcher);e!==n&&(this.errorState=e,this._stateChanges.next())}_getCurrentErrorState(n){if(this.formField&&n?.isSignalErrorState)return n.isSignalErrorState(this.formField.field())??!1;let e=this._parentFormGroup||this._parentForm,i=this.ngControl?this.ngControl.control:null;return n?.isErrorState(i,e)??!1}};var fw=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({imports:[ve]})}return t})();var Rv=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({imports:[ar,fw,hr,ve]})}return t})();var S1=["trigger"],C1=["panel"],D1=[[["mat-select-trigger"]],"*"],E1=["mat-select-trigger","*"];function w1(t,n){if(t&1&&(m(0,"span",4),y(1),g()),t&2){let e=w();_(),Te(e.placeholder)}}function x1(t,n){t&1&&he(0)}function I1(t,n){if(t&1&&(m(0,"span",11),y(1),g()),t&2){let e=w(2);_(),Te(e.triggerValue)}}function M1(t,n){if(t&1&&(m(0,"span",5),V(1,x1,1,0)(2,I1,2,1,"span",11),g()),t&2){let e=w();_(),B(e.customTrigger?1:2)}}function T1(t,n){if(t&1){let e=Le();m(0,"div",12,1),D("keydown",function(r){ee(e);let o=w();return te(o._handleKeydown(r))}),he(2,1),g()}if(t&2){let e=w();wt(e.panelClass),z("mat-select-panel-animations-enabled",!e._animationsDisabled)("mat-primary",e._parentFormField?.color==="primary")("mat-accent",e._parentFormField?.color==="accent")("mat-warn",e._parentFormField?.color==="warn")("mat-undefined",!e._parentFormField?.color),re("id",e.id+"-panel")("aria-multiselectable",e.multiple)("aria-label",e.ariaLabel||null)("aria-labelledby",e._getPanelAriaLabelledby())}}var N1=new b("mat-select-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(R);return()=>Si(t)}}),k1=new b("MAT_SELECT_CONFIG"),A1=new b("MatSelectTrigger"),Ov=class{source;value;constructor(n,e){this.source=n,this.value=e}},wf=(()=>{class t{_viewportRuler=d(yi);_changeDetectorRef=d(Oe);_elementRef=d(O);_dir=d(ct,{optional:!0});_idGenerator=d(xe);_renderer=d(Re);_parentFormField=d(Tl,{optional:!0});ngControl=d(ks,{self:!0,optional:!0});_liveAnnouncer=d(vl);_defaultOptions=d(k1,{optional:!0});_animationsDisabled=Ce();_popoverLocation;_initialized=new E;_cleanupDetach;options;optionGroups;customTrigger;_positions=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"}];_scrollOptionIntoView(e){let i=this.options.toArray()[e];if(i){let r=this.panel.nativeElement,o=dw(e,this.options,this.optionGroups),s=i._getHostElement();e===0&&o===1?r.scrollTop=0:r.scrollTop=uw(s.offsetTop,s.offsetHeight,r.scrollTop,r.offsetHeight)}}_positioningSettled(){this._scrollOptionIntoView(this._keyManager.activeItemIndex||0)}_getChangeEvent(e){return new Ov(this,e)}_scrollStrategyFactory=d(N1);_panelOpen=!1;_compareWith=(e,i)=>e===i;_uid=this._idGenerator.getId("mat-select-");_triggerAriaLabelledBy=null;_previousControl;_destroy=new E;_errorStateTracker;stateChanges=new E;disableAutomaticLabeling=!0;userAriaDescribedBy;_selectionModel;_keyManager;_preferredOverlayOrigin;_overlayWidth;_onChange=()=>{};_onTouched=()=>{};_valueId=this._idGenerator.getId("mat-select-value-");_scrollStrategy;_overlayPanelClass=this._defaultOptions?.overlayPanelClass||"";get focused(){return this._focused||this._panelOpen}_focused=!1;controlType="mat-select";trigger;panel;_overlayDir;panelClass;disabled=!1;get disableRipple(){return this._disableRipple()}set disableRipple(e){this._disableRipple.set(e)}_disableRipple=j(!1);tabIndex=0;get hideSingleSelectionIndicator(){return this._hideSingleSelectionIndicator}set hideSingleSelectionIndicator(e){this._hideSingleSelectionIndicator=e,this._syncParentProperties()}_hideSingleSelectionIndicator=this._defaultOptions?.hideSingleSelectionIndicator??!1;get placeholder(){return this._placeholder}set placeholder(e){this._placeholder=e,this.stateChanges.next()}_placeholder;get required(){return this._required??this.ngControl?.control?.hasValidator(yo.required)??!1}set required(e){this._required=e,this.stateChanges.next()}_required;get multiple(){return this._multiple}set multiple(e){this._selectionModel,this._multiple=e}_multiple=!1;disableOptionCentering=this._defaultOptions?.disableOptionCentering??!1;get compareWith(){return this._compareWith}set compareWith(e){this._compareWith=e,this._selectionModel&&this._initializeSelection()}get value(){return this._value}set value(e){this._assignValue(e)&&this._onChange(e)}_value;ariaLabel="";ariaLabelledby;get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}typeaheadDebounceInterval;sortComparator;get id(){return this._id}set id(e){this._id=e||this._uid,this.stateChanges.next()}_id;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}panelWidth=this._defaultOptions&&typeof this._defaultOptions.panelWidth<"u"?this._defaultOptions.panelWidth:"auto";canSelectNullableOptions=this._defaultOptions?.canSelectNullableOptions??!1;optionSelectionChanges=cn(()=>{let e=this.options;return e?e.changes.pipe(nt(e),Xe(()=>Nt(...e.map(i=>i.onSelectionChange)))):this._initialized.pipe(Xe(()=>this.optionSelectionChanges))});openedChange=new W;_openedStream=this.openedChange.pipe(ge(e=>e),oe(()=>{}));_closedStream=this.openedChange.pipe(ge(e=>!e),oe(()=>{}));selectionChange=new W;valueChange=new W;constructor(){let e=d(Ef),i=d(Bl,{optional:!0}),r=d(jl,{optional:!0}),o=d(new _n("tabindex"),{optional:!0}),s=d(yl,{optional:!0}),a=d(Cf,{optional:!0,self:!0});this.ngControl&&(this.ngControl.valueAccessor=this),this._defaultOptions?.typeaheadDebounceInterval!=null&&(this.typeaheadDebounceInterval=this._defaultOptions.typeaheadDebounceInterval),this._errorStateTracker=new As(e,a||this.ngControl,r,i,this.stateChanges),this._scrollStrategy=this._scrollStrategyFactory(),this.tabIndex=o==null?0:parseInt(o)||0,this._popoverLocation=s?.usePopover===!1?null:"inline",this.id=this.id}ngOnInit(){this._selectionModel=new _o(this.multiple),this.stateChanges.next(),this._viewportRuler.change().pipe(ye(this._destroy)).subscribe(()=>{this.panelOpen&&(this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._changeDetectorRef.detectChanges())})}ngAfterContentInit(){this._initialized.next(),this._initialized.complete(),this._initKeyManager(),this._selectionModel.changed.pipe(ye(this._destroy)).subscribe(e=>{e.added.forEach(i=>i.select()),e.removed.forEach(i=>i.deselect())}),this.options.changes.pipe(nt(null),ye(this._destroy)).subscribe(()=>{this._resetOptions(),this._initializeSelection()})}ngDoCheck(){let e=this._getTriggerAriaLabelledby(),i=this.ngControl;if(e!==this._triggerAriaLabelledBy){let r=this._elementRef.nativeElement;this._triggerAriaLabelledBy=e,e?r.setAttribute("aria-labelledby",e):r.removeAttribute("aria-labelledby")}i&&(this._previousControl!==i.control&&(this._previousControl!==void 0&&i.disabled!==null&&i.disabled!==this.disabled&&(this.disabled=i.disabled),this._previousControl=i.control),this.updateErrorState())}ngOnChanges(e){(e.disabled||e.userAriaDescribedBy)&&this.stateChanges.next(),e.typeaheadDebounceInterval&&this._keyManager&&this._keyManager.withTypeAhead(this.typeaheadDebounceInterval),e.panelClass&&this.panelClass instanceof Set&&(this.panelClass=Array.from(this.panelClass))}ngOnDestroy(){this._cleanupDetach?.(),this._keyManager?.destroy(),this._destroy.next(),this._destroy.complete(),this.stateChanges.complete()}toggle(){this.panelOpen?this.close():this.open()}open(){this._canOpen()&&(this._parentFormField&&(this._preferredOverlayOrigin=this._parentFormField.getConnectedOverlayOrigin()),this._cleanupDetach?.(),this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._panelOpen=!0,this._overlayDir.positionChange.pipe(Ae(1)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this._positioningSettled()}),this._overlayDir.attachOverlay(),this._keyManager.withHorizontalOrientation(null),this._highlightCorrectOption(),this._changeDetectorRef.markForCheck(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!0)))}close(){this._panelOpen&&(this._panelOpen=!1,this._exitAndDetach(),this._keyManager.withHorizontalOrientation(this._isRtl()?"rtl":"ltr"),this._changeDetectorRef.markForCheck(),this._onTouched(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!1)))}_exitAndDetach(){if(this._animationsDisabled||!this.panel){this._detachOverlay();return}this._cleanupDetach?.(),this._cleanupDetach=()=>{i(),clearTimeout(r),this._cleanupDetach=void 0};let e=this.panel.nativeElement,i=this._renderer.listen(e,"animationend",o=>{o.animationName==="_mat-select-exit"&&(this._cleanupDetach?.(),this._detachOverlay())}),r=setTimeout(()=>{this._cleanupDetach?.(),this._detachOverlay()},200);e.classList.add("mat-select-panel-exit")}_detachOverlay(){this._overlayDir.detachOverlay(),this._changeDetectorRef.markForCheck()}writeValue(e){this._assignValue(e)}registerOnChange(e){this._onChange=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e,this._changeDetectorRef.markForCheck(),this.stateChanges.next()}get panelOpen(){return this._panelOpen}get selected(){return this.multiple?this._selectionModel?.selected||[]:this._selectionModel?.selected[0]}get triggerValue(){if(this.empty)return"";if(this._multiple){let e=this._selectionModel.selected.map(i=>i.viewValue);return this._isRtl()&&e.reverse(),e.join(", ")}return this._selectionModel.selected[0].viewValue}updateErrorState(){this._errorStateTracker.updateErrorState()}_isRtl(){return this._dir?this._dir.value==="rtl":!1}_handleKeydown(e){this.disabled||(this.panelOpen?this._handleOpenKeydown(e):this._handleClosedKeydown(e))}_handleClosedKeydown(e){let i=e.keyCode,r=i===40||i===38||i===37||i===39,o=i===13||i===32,s=this._keyManager;if(!s.isTyping()&&o&&!$e(e)||(this.multiple||e.altKey)&&r)e.preventDefault(),this.open();else if(!this.multiple){let a=this.selected;s.onKeydown(e);let l=this.selected;l&&a!==l&&this._liveAnnouncer.announce(l.viewValue,1e4)}}_handleOpenKeydown(e){let i=this._keyManager,r=e.keyCode,o=r===40||r===38,s=i.isTyping();if(o&&e.altKey)e.preventDefault(),this.close();else if(!s&&(r===13||r===32)&&i.activeItem&&!$e(e))e.preventDefault(),i.activeItem._selectViaInteraction();else if(!s&&this._multiple&&r===65&&e.ctrlKey){e.preventDefault();let a=this.options.some(l=>!l.disabled&&!l.selected);this.options.forEach(l=>{l.disabled||(a?l.select():l.deselect())})}else{let a=i.activeItemIndex;i.onKeydown(e),this._multiple&&o&&e.shiftKey&&i.activeItem&&i.activeItemIndex!==a&&i.activeItem._selectViaInteraction()}}_handleOverlayKeydown(e){e.keyCode===27&&!$e(e)&&(e.preventDefault(),this.close())}_onFocus(){this.disabled||(this._focused=!0,this.stateChanges.next())}_onBlur(){this._focused=!1,this._keyManager?.cancelTypeahead(),!this.disabled&&!this.panelOpen&&(this._onTouched(),this._changeDetectorRef.markForCheck(),this.stateChanges.next())}get empty(){return!this._selectionModel||this._selectionModel.isEmpty()}_initializeSelection(){Promise.resolve().then(()=>{this.ngControl&&(this._value=this.ngControl.value),this._setSelectionByValue(this._value),this.stateChanges.next()})}_setSelectionByValue(e){if(this.options.forEach(i=>i.setInactiveStyles()),this._selectionModel.clear(),this.multiple&&e)Array.isArray(e),e.forEach(i=>this._selectOptionByValue(i)),this._sortValues();else{let i=this._selectOptionByValue(e);i?this._keyManager.updateActiveItem(i):this.panelOpen||this._keyManager.updateActiveItem(-1)}this._changeDetectorRef.markForCheck()}_selectOptionByValue(e){let i=this.options.find(r=>{if(this._selectionModel.isSelected(r))return!1;try{return(r.value!=null||this.canSelectNullableOptions)&&this._compareWith(r.value,e)}catch{return!1}});return i&&this._selectionModel.select(i),i}_assignValue(e){return e!==this._value||this._multiple&&Array.isArray(e)?(this.options&&this._setSelectionByValue(e),this._value=e,!0):!1}_skipPredicate=e=>this.panelOpen?!1:e.disabled;_getOverlayWidth(e){return this.panelWidth==="auto"?(e instanceof _s?e.elementRef:e||this._elementRef).nativeElement.getBoundingClientRect().width:this.panelWidth===null?"":this.panelWidth}_syncParentProperties(){if(this.options)for(let e of this.options)e._changeDetectorRef.markForCheck()}_initKeyManager(){this._keyManager=new Dl(this.options).withTypeAhead(this.typeaheadDebounceInterval).withVerticalOrientation().withHorizontalOrientation(this._isRtl()?"rtl":"ltr").withHomeAndEnd().withPageUpDown().withAllowedModifierKeys(["shiftKey"]).skipPredicate(this._skipPredicate),this._keyManager.tabOut.subscribe(()=>{this.panelOpen&&(!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction(),this.focus(),this.close())}),this._keyManager.change.subscribe(()=>{this._panelOpen&&this.panel?this._scrollOptionIntoView(this._keyManager.activeItemIndex||0):!this._panelOpen&&!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction()})}_resetOptions(){let e=Nt(this.options.changes,this._destroy);this.optionSelectionChanges.pipe(ye(e)).subscribe(i=>{this._onSelect(i.source,i.isUserInput),i.isUserInput&&!this.multiple&&this._panelOpen&&(this.close(),this.focus())}),Nt(...this.options.map(i=>i._stateChanges)).pipe(ye(e)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this.stateChanges.next()})}_onSelect(e,i){let r=this._selectionModel.isSelected(e);!this.canSelectNullableOptions&&e.value==null&&!this._multiple?(e.deselect(),this._selectionModel.clear(),this.value!=null&&this._propagateChanges(e.value)):(r!==e.selected&&(e.selected?this._selectionModel.select(e):this._selectionModel.deselect(e)),i&&this._keyManager.setActiveItem(e),this.multiple&&(this._sortValues(),i&&this.focus())),r!==this._selectionModel.isSelected(e)&&this._propagateChanges(),this.stateChanges.next()}_sortValues(){if(this.multiple){let e=this.options.toArray();this._selectionModel.sort((i,r)=>this.sortComparator?this.sortComparator(i,r,e):e.indexOf(i)-e.indexOf(r)),this.stateChanges.next()}}_propagateChanges(e){let i;this.multiple?i=this.selected.map(r=>r.value):i=this.selected?this.selected.value:e,this._value=i,this.valueChange.emit(i),this._onChange(i),this.selectionChange.emit(this._getChangeEvent(i)),this._changeDetectorRef.markForCheck()}_highlightCorrectOption(){if(this._keyManager)if(this.empty){let e=-1;for(let i=0;i<this.options.length;i++)if(!this.options.get(i).disabled){e=i;break}this._keyManager.setActiveItem(e)}else this._keyManager.setActiveItem(this._selectionModel.selected[0])}_canOpen(){return!this._panelOpen&&!this.disabled&&this.options?.length>0&&!!this._overlayDir}focus(e){this._elementRef.nativeElement.focus(e)}_getPanelAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||null,i=e?e+" ":"";return this.ariaLabelledby?i+this.ariaLabelledby:e}_getAriaActiveDescendant(){return this.panelOpen&&this._keyManager&&this._keyManager.activeItem?this._keyManager.activeItem.id:null}_getTriggerAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||"";return this.ariaLabelledby&&(e+=" "+this.ariaLabelledby),e||(e=this._valueId),e}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(e){let i=yt(e);i&&(i.tagName==="MAT-OPTION"||i.classList.contains("cdk-overlay-backdrop")||i.closest(".mat-mdc-select-panel"))||(this.focus(),this.open())}get shouldLabelFloat(){return this.panelOpen||!this.empty||this.focused&&!!this.placeholder}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-select"]],contentQueries:function(i,r,o){if(i&1&&zn(o,A1,5)(o,hr,5)(o,Av,5),i&2){let s;ce(s=de())&&(r.customTrigger=s.first),ce(s=de())&&(r.options=s),ce(s=de())&&(r.optionGroups=s)}},viewQuery:function(i,r){if(i&1&&Ue(S1,5)(C1,5)(Qu,5),i&2){let o;ce(o=de())&&(r.trigger=o.first),ce(o=de())&&(r.panel=o.first),ce(o=de())&&(r._overlayDir=o.first)}},hostAttrs:["role","combobox","aria-haspopup","listbox",1,"mat-mdc-select"],hostVars:21,hostBindings:function(i,r){i&1&&D("keydown",function(s){return r._handleKeydown(s)})("focus",function(){return r._onFocus()})("blur",function(){return r._onBlur()}),i&2&&(re("id",r.id)("tabindex",r.disabled?-1:r.tabIndex)("aria-controls",r.panelOpen?r.id+"-panel":null)("aria-expanded",r.panelOpen)("aria-label",r.ariaLabel||null)("aria-required",r.required.toString())("aria-disabled",r.disabled.toString())("aria-invalid",r.errorState)("aria-activedescendant",r._getAriaActiveDescendant()),z("mat-mdc-select-disabled",r.disabled)("mat-mdc-select-invalid",r.errorState)("mat-mdc-select-required",r.required)("mat-mdc-select-empty",r.empty)("mat-mdc-select-multiple",r.multiple)("mat-select-open",r.panelOpen))},inputs:{userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],panelClass:"panelClass",disabled:[2,"disabled","disabled",P],disableRipple:[2,"disableRipple","disableRipple",P],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:Yr(e)],hideSingleSelectionIndicator:[2,"hideSingleSelectionIndicator","hideSingleSelectionIndicator",P],placeholder:"placeholder",required:[2,"required","required",P],multiple:[2,"multiple","multiple",P],disableOptionCentering:[2,"disableOptionCentering","disableOptionCentering",P],compareWith:"compareWith",value:"value",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],errorStateMatcher:"errorStateMatcher",typeaheadDebounceInterval:[2,"typeaheadDebounceInterval","typeaheadDebounceInterval",Yr],sortComparator:"sortComparator",id:"id",panelWidth:"panelWidth",canSelectNullableOptions:[2,"canSelectNullableOptions","canSelectNullableOptions",P]},outputs:{openedChange:"openedChange",_openedStream:"opened",_closedStream:"closed",selectionChange:"selectionChange",valueChange:"valueChange"},exportAs:["matSelect"],features:[Be([{provide:Ml,useExisting:t},{provide:kv,useExisting:t}]),at],ngContentSelectors:E1,decls:11,vars:10,consts:[["fallbackOverlayOrigin","cdkOverlayOrigin","trigger",""],["panel",""],["cdk-overlay-origin","",1,"mat-mdc-select-trigger",3,"click"],[1,"mat-mdc-select-value"],[1,"mat-mdc-select-placeholder","mat-mdc-select-min-line"],[1,"mat-mdc-select-value-text"],[1,"mat-mdc-select-arrow-wrapper"],[1,"mat-mdc-select-arrow"],["viewBox","0 0 24 24","width","24px","height","24px","focusable","false","aria-hidden","true"],["d","M7 10l5 5 5-5z"],["cdk-connected-overlay","","cdkConnectedOverlayHasBackdrop","","cdkConnectedOverlayBackdropClass","cdk-overlay-transparent-backdrop",3,"detach","backdropClick","overlayKeydown","cdkConnectedOverlayDisableClose","cdkConnectedOverlayPanelClass","cdkConnectedOverlayScrollStrategy","cdkConnectedOverlayOrigin","cdkConnectedOverlayPositions","cdkConnectedOverlayWidth","cdkConnectedOverlayFlexibleDimensions","cdkConnectedOverlayUsePopover"],[1,"mat-mdc-select-min-line"],["role","listbox","tabindex","-1",1,"mat-mdc-select-panel","mdc-menu-surface","mdc-menu-surface--open",3,"keydown"]],template:function(i,r){if(i&1&&(Pe(D1),m(0,"div",2,0),D("click",function(){return r.open()}),m(3,"div",3),V(4,w1,2,1,"span",4)(5,M1,3,1,"span",5),g(),m(6,"div",6)(7,"div",7),Fi(),m(8,"svg",8),ie(9,"path",9),g()()()(),Dt(10,T1,3,16,"ng-template",10),D("detach",function(){return r.close()})("backdropClick",function(){return r.close()})("overlayKeydown",function(s){return r._handleOverlayKeydown(s)})),i&2){let o=ht(1);_(3),re("id",r._valueId),_(),B(r.empty?4:5),_(6),U("cdkConnectedOverlayDisableClose",!0)("cdkConnectedOverlayPanelClass",r._overlayPanelClass)("cdkConnectedOverlayScrollStrategy",r._scrollStrategy)("cdkConnectedOverlayOrigin",r._preferredOverlayOrigin||o)("cdkConnectedOverlayPositions",r._positions)("cdkConnectedOverlayWidth",r._overlayWidth)("cdkConnectedOverlayFlexibleDimensions",!0)("cdkConnectedOverlayUsePopover",r._popoverLocation)}},dependencies:[_s,Qu],styles:[`@keyframes _mat-select-enter {
  from {
    opacity: 0;
    transform: scaleY(0.8);
  }
  to {
    opacity: 1;
    transform: none;
  }
}
@keyframes _mat-select-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-select {
  display: inline-block;
  width: 100%;
  outline: none;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  color: var(--%NS%mat-select-enabled-trigger-text-color, var(--%NS%mat-sys-on-surface));
  font-family: var(--%NS%mat-select-trigger-text-font, var(--%NS%mat-sys-body-large-font));
  line-height: var(--%NS%mat-select-trigger-text-line-height, var(--%NS%mat-sys-body-large-line-height));
  font-size: var(--%NS%mat-select-trigger-text-size, var(--%NS%mat-sys-body-large-size));
  font-weight: var(--%NS%mat-select-trigger-text-weight, var(--%NS%mat-sys-body-large-weight));
  letter-spacing: var(--%NS%mat-select-trigger-text-tracking, var(--%NS%mat-sys-body-large-tracking));
}

div.mat-mdc-select-panel {
  box-shadow: var(--%NS%mat-select-container-elevation-shadow, 0px 3px 1px -2px rgba(0, 0, 0, 0.2), 0px 2px 2px 0px rgba(0, 0, 0, 0.14), 0px 1px 5px 0px rgba(0, 0, 0, 0.12));
}

.mat-mdc-select-disabled {
  color: var(--%NS%mat-select-disabled-trigger-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-select-disabled .mat-mdc-select-placeholder {
  color: var(--%NS%mat-select-disabled-trigger-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-select-trigger {
  display: inline-flex;
  align-items: center;
  cursor: pointer;
  position: relative;
  box-sizing: border-box;
  width: 100%;
}
.mat-mdc-select-disabled .mat-mdc-select-trigger {
  -webkit-user-select: none;
  user-select: none;
  cursor: default;
}

.mat-mdc-select-value {
  width: 100%;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.mat-mdc-select-value-text {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.mat-mdc-select-arrow-wrapper {
  height: 24px;
  flex-shrink: 0;
  display: inline-flex;
  align-items: center;
}
.mat-form-field-appearance-fill .mdc-text-field--no-label .mat-mdc-select-arrow-wrapper {
  transform: none;
}

.mat-mdc-form-field .mat-mdc-select.mat-mdc-select-invalid .mat-mdc-select-arrow,
.mat-form-field-invalid:not(.mat-form-field-disabled) .mat-mdc-form-field-infix::after {
  color: var(--%NS%mat-select-invalid-arrow-color, var(--%NS%mat-sys-error));
}

.mat-mdc-select-arrow {
  width: 10px;
  height: 5px;
  position: relative;
  color: var(--%NS%mat-select-enabled-arrow-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-form-field.mat-focused .mat-mdc-select-arrow {
  color: var(--%NS%mat-select-focused-arrow-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-form-field .mat-mdc-select.mat-mdc-select-disabled .mat-mdc-select-arrow {
  color: var(--%NS%mat-select-disabled-arrow-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-select-open .mat-mdc-select-arrow {
  transform: rotate(180deg);
}
.mat-form-field-animations-enabled .mat-mdc-select-arrow {
  transition: transform 80ms linear;
}
.mat-mdc-select-arrow svg {
  fill: currentColor;
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
}
@media (forced-colors: active) {
  .mat-mdc-select-arrow svg {
    fill: CanvasText;
  }
  .mat-mdc-select-disabled .mat-mdc-select-arrow svg {
    fill: GrayText;
  }
}

div.mat-mdc-select-panel {
  width: 100%;
  max-height: 275px;
  outline: 0;
  overflow: auto;
  padding: 8px 0;
  box-sizing: border-box;
  transform-origin: top center;
  border-radius: 0 0 4px 4px;
  position: relative;
  background-color: var(--%NS%mat-select-panel-background-color, var(--%NS%mat-sys-surface-container));
}
.mat-mdc-select-panel-above div.mat-mdc-select-panel {
  border-radius: 4px 4px 0 0;
  transform-origin: bottom center;
}
@media (forced-colors: active) {
  div.mat-mdc-select-panel {
    outline: solid 1px;
  }
}

.mat-select-panel-animations-enabled {
  animation: _mat-select-enter 120ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-select-panel-animations-enabled.mat-select-panel-exit {
  animation: _mat-select-exit 100ms linear;
}

.mat-mdc-select-placeholder {
  transition: color 400ms 133.3333333333ms cubic-bezier(0.25, 0.8, 0.25, 1);
  color: var(--%NS%mat-select-placeholder-text-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-form-field:not(.mat-form-field-animations-enabled) .mat-mdc-select-placeholder, ._mat-animation-noopable .mat-mdc-select-placeholder {
  transition: none;
}
.mat-form-field-hide-placeholder .mat-mdc-select-placeholder {
  color: transparent;
  -webkit-text-fill-color: transparent;
  transition: none;
  display: block;
}

.mat-mdc-form-field-type-mat-select:not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper {
  cursor: pointer;
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-fill .mat-mdc-floating-label {
  max-width: calc(100% - 18px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-fill .mdc-floating-label--float-above {
  max-width: calc(100% / 0.75 - 24px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-outline .mdc-notched-outline__notch {
  max-width: calc(100% - 60px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-outline .mdc-text-field--label-floating .mdc-notched-outline__notch {
  max-width: calc(100% - 24px);
}

.mat-mdc-select-min-line:empty::before {
  content: " ";
  white-space: pre;
  width: 1px;
  display: inline-block;
  visibility: hidden;
}

.mat-form-field-appearance-fill .mat-mdc-select-arrow-wrapper {
  transform: var(--%NS%mat-select-arrow-transform, translateY(-8px));
}
`],encapsulation:2})}return t})();var xf=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({imports:[on,Rv,ve,_i,Tn,Rv]})}return t})();function R1(t,n){if(t&1){let e=Le();m(0,"div",1)(1,"button",2),D("click",function(){ee(e);let r=w();return te(r.action())}),y(2),g()()}if(t&2){let e=w();_(2),At(" ",e.data.action," ")}}var O1=["label"];function P1(t,n){}var F1=Math.pow(2,31)-1,Hl=class{_overlayRef;instance;containerInstance;_afterDismissed=new E;_afterOpened=new E;_onAction=new E;_durationTimeoutId;_dismissedByAction=!1;constructor(n,e){this._overlayRef=e,this.containerInstance=n,n._onExit.subscribe(()=>this._finishDismiss())}dismiss(){this._afterDismissed.closed||this.containerInstance.exit(),clearTimeout(this._durationTimeoutId)}dismissWithAction(){this._onAction.closed||(this._dismissedByAction=!0,this._onAction.next(),this._onAction.complete(),this.dismiss()),clearTimeout(this._durationTimeoutId)}closeWithAction(){this.dismissWithAction()}_dismissAfter(n){this._durationTimeoutId=setTimeout(()=>this.dismiss(),Math.min(n,F1))}_open(){this._afterOpened.closed||(this._afterOpened.next(),this._afterOpened.complete())}_finishDismiss(){this._overlayRef.dispose(),this._onAction.closed||this._onAction.complete(),this._afterDismissed.next({dismissedByAction:this._dismissedByAction}),this._afterDismissed.complete(),this._dismissedByAction=!1}afterDismissed(){return this._afterDismissed}afterOpened(){return this.containerInstance._onEnter}onAction(){return this._onAction}},mw=new b("MatSnackBarData"),Rs=class{politeness="polite";announcementMessage="";viewContainerRef;duration=0;panelClass;direction;data=null;horizontalPosition="center";verticalPosition="bottom"},L1=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,selectors:[["","matSnackBarLabel",""]],hostAttrs:[1,"mat-mdc-snack-bar-label","mdc-snackbar__label"]})}return t})(),V1=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,selectors:[["","matSnackBarActions",""]],hostAttrs:[1,"mat-mdc-snack-bar-actions","mdc-snackbar__actions"]})}return t})(),B1=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,selectors:[["","matSnackBarAction",""]],hostAttrs:[1,"mat-mdc-snack-bar-action","mdc-snackbar__action"]})}return t})(),j1=(()=>{class t{snackBarRef=d(Hl);data=d(mw);action(){this.snackBarRef.dismissWithAction()}get hasAction(){return!!this.data.action}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["simple-snack-bar"]],hostAttrs:[1,"mat-mdc-simple-snack-bar"],exportAs:["matSnackBar"],decls:3,vars:2,consts:[["matSnackBarLabel",""],["matSnackBarActions",""],["matButton","","matSnackBarAction","",3,"click"]],template:function(i,r){i&1&&(m(0,"div",0),y(1),g(),V(2,R1,3,1,"div",1)),i&2&&(_(),At(" ",r.data.message,`
`),_(),B(r.hasAction?2:-1))},dependencies:[lr,L1,V1,B1],styles:[`.mat-mdc-simple-snack-bar {
  display: flex;
}
.mat-mdc-simple-snack-bar .mat-mdc-snack-bar-label {
  max-height: 50vh;
  overflow: auto;
}
`],encapsulation:2})}return t})(),Pv="_mat-snack-bar-enter",Fv="_mat-snack-bar-exit",H1=(()=>{class t extends tr{_ngZone=d(N);_elementRef=d(O);_changeDetectorRef=d(Oe);_platform=d(be);_animationsDisabled=Ce();snackBarConfig=d(Rs);_document=d(L);_trackedModals=new Set;_enterFallback;_exitFallback;_injector=d(R);_announceDelay=150;_announceTimeoutId;_destroyed=!1;_portalOutlet;_onAnnounce=new E;_onExit=new E;_onEnter=new E;_animationState="void";_live;_label;_role;_liveElementId=d(xe).getId("mat-snack-bar-container-live-");constructor(){super();let e=this.snackBarConfig;e.politeness==="assertive"&&!e.announcementMessage?this._live="assertive":e.politeness==="off"?this._live="off":this._live="polite",this._platform.FIREFOX&&(this._live==="polite"&&(this._role="status"),this._live==="assertive"&&(this._role="alert"))}attachComponentPortal(e){this._assertNotAttached();let i=this._portalOutlet.attachComponentPortal(e);return this._afterPortalAttached(),i}attachTemplatePortal(e){this._assertNotAttached();let i=this._portalOutlet.attachTemplatePortal(e);return this._afterPortalAttached(),i}attachDomPortal=e=>{this._assertNotAttached();let i=this._portalOutlet.attachDomPortal(e);return this._afterPortalAttached(),i};onAnimationEnd(e){e===Fv?this._completeExit():e===Pv&&(clearTimeout(this._enterFallback),this._ngZone.run(()=>{this._onEnter.next(),this._onEnter.complete()}))}enter(){this._destroyed||(this._animationState="visible",this._changeDetectorRef.markForCheck(),this._changeDetectorRef.detectChanges(),this._screenReaderAnnounce(),this._animationsDisabled?Qe(()=>{this._ngZone.run(()=>queueMicrotask(()=>this.onAnimationEnd(Pv)))},{injector:this._injector}):(clearTimeout(this._enterFallback),this._enterFallback=setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-snack-bar-fallback-visible"),this.onAnimationEnd(Pv)},200)))}exit(){return this._destroyed?Y(void 0):(this._ngZone.run(()=>{this._animationState="hidden",this._changeDetectorRef.markForCheck(),this._elementRef.nativeElement.setAttribute("mat-exit",""),clearTimeout(this._announceTimeoutId),this._animationsDisabled?Qe(()=>{this._ngZone.run(()=>queueMicrotask(()=>this.onAnimationEnd(Fv)))},{injector:this._injector}):(clearTimeout(this._exitFallback),this._exitFallback=setTimeout(()=>this.onAnimationEnd(Fv),200))}),this._onExit)}ngOnDestroy(){this._destroyed=!0,this._clearFromModals(),this._completeExit()}_completeExit(){clearTimeout(this._exitFallback),queueMicrotask(()=>{this._onExit.next(),this._onExit.complete()})}_afterPortalAttached(){let e=this._elementRef.nativeElement,i=this.snackBarConfig.panelClass;i&&(Array.isArray(i)?i.forEach(s=>e.classList.add(s)):e.classList.add(i)),this._exposeToModals();let r=this._label.nativeElement,o="mdc-snackbar__label";r.classList.toggle(o,!r.querySelector(`.${o}`))}_exposeToModals(){let e=this._liveElementId,i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],s=o.getAttribute("aria-owns");this._trackedModals.add(o),s?s.indexOf(e)===-1&&o.setAttribute("aria-owns",s+" "+e):o.setAttribute("aria-owns",e)}}_clearFromModals(){this._trackedModals.forEach(e=>{let i=e.getAttribute("aria-owns");if(i){let r=i.replace(this._liveElementId,"").trim();r.length>0?e.setAttribute("aria-owns",r):e.removeAttribute("aria-owns")}}),this._trackedModals.clear()}_assertNotAttached(){this._portalOutlet.hasAttached()}_screenReaderAnnounce(){this._announceTimeoutId||this._ngZone.runOutsideAngular(()=>{this._announceTimeoutId=setTimeout(()=>{if(this._destroyed)return;let e=this._elementRef.nativeElement,i=e.querySelector("[aria-hidden]"),r=e.querySelector("[aria-live]");if(i&&r){let o=null;this._platform.isBrowser&&document.activeElement instanceof HTMLElement&&i.contains(document.activeElement)&&(o=document.activeElement),i.removeAttribute("aria-hidden"),r.appendChild(i),o?.focus(),this._onAnnounce.next(),this._onAnnounce.complete()}},this._announceDelay)})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-snack-bar-container"]],viewQuery:function(i,r){if(i&1&&Ue(Gn,7)(O1,7),i&2){let o;ce(o=de())&&(r._portalOutlet=o.first),ce(o=de())&&(r._label=o.first)}},hostAttrs:[1,"mdc-snackbar","mat-mdc-snack-bar-container"],hostVars:6,hostBindings:function(i,r){i&1&&D("animationend",function(s){return r.onAnimationEnd(s.animationName)})("animationcancel",function(s){return r.onAnimationEnd(s.animationName)}),i&2&&z("mat-snack-bar-container-enter",r._animationState==="visible")("mat-snack-bar-container-exit",r._animationState==="hidden")("mat-snack-bar-container-animations-enabled",!r._animationsDisabled)},features:[Me],decls:6,vars:3,consts:[["label",""],[1,"mdc-snackbar__surface","mat-mdc-snackbar-surface"],[1,"mat-mdc-snack-bar-label"],["aria-hidden","true"],["cdkPortalOutlet",""]],template:function(i,r){i&1&&(m(0,"div",1)(1,"div",2,0)(3,"div",3),Dt(4,P1,0,0,"ng-template",4),g(),ie(5,"div"),g()()),i&2&&(_(5),re("aria-live",r._live)("role",r._role)("id",r._liveElementId))},dependencies:[Gn],styles:[`@keyframes _mat-snack-bar-enter {
  from {
    transform: scale(0.8);
    opacity: 0;
  }
  to {
    transform: scale(1);
    opacity: 1;
  }
}
@keyframes _mat-snack-bar-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-snack-bar-container {
  display: flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  -webkit-tap-highlight-color: rgba(0, 0, 0, 0);
  margin: 8px;
}
.mat-mdc-snack-bar-handset .mat-mdc-snack-bar-container {
  width: 100vw;
}

.mat-snack-bar-container-animations-enabled {
  opacity: 0;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-fallback-visible {
  opacity: 1;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-container-enter {
  animation: _mat-snack-bar-enter 150ms cubic-bezier(0, 0, 0.2, 1) forwards;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-container-exit {
  animation: _mat-snack-bar-exit 75ms cubic-bezier(0.4, 0, 1, 1) forwards;
}

.mat-mdc-snackbar-surface {
  box-shadow: 0px 3px 5px -1px rgba(0, 0, 0, 0.2), 0px 6px 10px 0px rgba(0, 0, 0, 0.14), 0px 1px 18px 0px rgba(0, 0, 0, 0.12);
  display: flex;
  align-items: center;
  justify-content: flex-start;
  box-sizing: border-box;
  padding-left: 0;
  padding-right: 8px;
}
[dir=rtl] .mat-mdc-snackbar-surface {
  padding-right: 0;
  padding-left: 8px;
}
.mat-mdc-snack-bar-container .mat-mdc-snackbar-surface {
  min-width: 344px;
  max-width: 672px;
}
.mat-mdc-snack-bar-handset .mat-mdc-snackbar-surface {
  width: 100%;
  min-width: 0;
}
@media (forced-colors: active) {
  .mat-mdc-snackbar-surface {
    outline: solid 1px;
  }
}
.mat-mdc-snack-bar-container .mat-mdc-snackbar-surface {
  color: var(--%NS%mat-snack-bar-supporting-text-color, var(--%NS%mat-sys-inverse-on-surface));
  border-radius: var(--%NS%mat-snack-bar-container-shape, var(--%NS%mat-sys-corner-extra-small));
  background-color: var(--%NS%mat-snack-bar-container-color, var(--%NS%mat-sys-inverse-surface));
}

.mdc-snackbar__label {
  width: 100%;
  flex-grow: 1;
  box-sizing: border-box;
  margin: 0;
  padding: 14px 8px 14px 16px;
}
[dir=rtl] .mdc-snackbar__label {
  padding-left: 8px;
  padding-right: 16px;
}
.mat-mdc-snack-bar-container .mdc-snackbar__label {
  font-family: var(--%NS%mat-snack-bar-supporting-text-font, var(--%NS%mat-sys-body-medium-font));
  font-size: var(--%NS%mat-snack-bar-supporting-text-size, var(--%NS%mat-sys-body-medium-size));
  font-weight: var(--%NS%mat-snack-bar-supporting-text-weight, var(--%NS%mat-sys-body-medium-weight));
  line-height: var(--%NS%mat-snack-bar-supporting-text-line-height, var(--%NS%mat-sys-body-medium-line-height));
}

.mat-mdc-snack-bar-actions {
  display: flex;
  flex-shrink: 0;
  align-items: center;
  box-sizing: border-box;
}

.mat-mdc-snack-bar-handset,
.mat-mdc-snack-bar-container,
.mat-mdc-snack-bar-label {
  flex: 1 1 auto;
}

.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled).mat-unthemed {
  color: var(--%NS%mat-snack-bar-button-color, var(--%NS%mat-sys-inverse-primary));
}
.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled) {
  --%NS%mat-button-text-state-layer-color: currentColor;
  --%NS%mat-button-text-ripple-color: currentColor;
}
.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled) .mat-ripple-element {
  opacity: 0.1;
}
`],encapsulation:2,changeDetection:1})}return t})(),U1=new b("mat-snack-bar-default-options",{providedIn:"root",factory:()=>new Rs}),pw=(()=>{class t{_live=d(vl);_injector=d(R);_breakpointObserver=d(lo);_parentSnackBar=d(t,{optional:!0,skipSelf:!0});_defaultConfig=d(U1);_animationsDisabled=Ce();_snackBarRefAtThisLevel=null;simpleSnackBarComponent=j1;snackBarContainerComponent=H1;handsetCssClass="mat-mdc-snack-bar-handset";get _openedSnackBarRef(){let e=this._parentSnackBar;return e?e._openedSnackBarRef:this._snackBarRefAtThisLevel}set _openedSnackBarRef(e){this._parentSnackBar?this._parentSnackBar._openedSnackBarRef=e:this._snackBarRefAtThisLevel=e}openFromComponent(e,i){return this._attach(e,i)}openFromTemplate(e,i){return this._attach(e,i)}open(e,i="",r){let o=v(v({},this._defaultConfig),r);return o.data={message:e,action:i},o.announcementMessage===e&&(o.announcementMessage=void 0),this.openFromComponent(this.simpleSnackBarComponent,o)}dismiss(){this._openedSnackBarRef&&this._openedSnackBarRef.dismiss()}ngOnDestroy(){this._snackBarRefAtThisLevel&&this._snackBarRefAtThisLevel.dismiss()}_attachSnackBarContainer(e,i){let r=i&&i.viewContainerRef&&i.viewContainerRef.injector,o=R.create({parent:r||this._injector,providers:[{provide:Rs,useValue:i}]}),s=new Cn(this.snackBarContainerComponent,i.viewContainerRef,o),a=e.attach(s);return a.instance.snackBarConfig=i,a.instance}_attach(e,i){let r=v(v(v({},new Rs),this._defaultConfig),i),o=this._createOverlay(r),s=this._attachSnackBarContainer(o,r),a=new Hl(s,o);if(e instanceof Ct){let l=new Dn(e,null,{$implicit:r.data,snackBarRef:a});a.instance=s.attachTemplatePortal(l)}else{let l=this._createInjector(r,a),c=new Cn(e,void 0,l),u=s.attachComponentPortal(c);a.instance=u.instance}return this._breakpointObserver.observe(Ci.HandsetPortrait).pipe(ye(o.detachments())).subscribe(l=>{o.overlayElement.classList.toggle(this.handsetCssClass,l.matches)}),r.announcementMessage&&s._onAnnounce.subscribe(()=>{this._live.announce(r.announcementMessage,r.politeness)}),this._animateSnackBar(a,r),this._openedSnackBarRef=a,this._openedSnackBarRef}_animateSnackBar(e,i){e.afterDismissed().subscribe(()=>{this._openedSnackBarRef==e&&(this._openedSnackBarRef=null),i.announcementMessage&&this._live.clear()}),i.duration&&i.duration>0&&e.afterOpened().subscribe(()=>e._dismissAfter(i.duration)),this._openedSnackBarRef?(this._openedSnackBarRef.afterDismissed().subscribe(()=>{e.containerInstance.enter()}),this._openedSnackBarRef.dismiss()):e.containerInstance.enter()}_createOverlay(e){let i=new wn;i.direction=e.direction;let r=Wn(this._injector),o=e.direction==="rtl",s=e.horizontalPosition==="left"||e.horizontalPosition==="start"&&!o||e.horizontalPosition==="end"&&o,a=!s&&e.horizontalPosition!=="center";return s?r.left("0"):a?r.right("0"):r.centerHorizontally(),e.verticalPosition==="top"?r.top("0"):r.bottom("0"),i.positionStrategy=r,i.disableAnimations=this._animationsDisabled,In(this._injector,i)}_createInjector(e,i){let r=e&&e.viewContainerRef&&e.viewContainerRef.injector;return R.create({parent:r||this._injector,providers:[{provide:Hl,useValue:i},{provide:mw,useValue:e.data}]})}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();function gw(t,n){let e=new Blob([JSON.stringify(n,null,2)],{type:"application/json"}),i=URL.createObjectURL(e);try{let r=document.createElement("a");r.href=i,r.download=t,r.click()}finally{URL.revokeObjectURL(i)}}function vw(t){return new Promise((n,e)=>{let i=new FileReader;i.onload=()=>{try{n(JSON.parse(i.result))}catch(r){e(r)}},i.onerror=()=>e(i.error??new Error("Falha ao ler o arquivo.")),i.readAsText(t)})}function z1(){return`${Date.now().toString(36)}-${Math.random().toString(36).slice(2,10)}`}function bw(t){return{id:z1(),name:t.name,savedAt:new Date().toISOString(),sizePresetKey:t.sizePresetKey,elements:t.elements}}var _w="zpl-label-studio.saved-labels.v1",$1="zpl-label-studio-etiquetas.json",Ul=class extends Error{},If=class t{savedLabelsSignal=j(us(_w)??[]);savedLabels=this.savedLabelsSignal.asReadonly();save(n,e){let i=bw({name:n,sizePresetKey:e.sizePreset.key,elements:e.elements}),r=[...this.savedLabelsSignal(),i];return this.persist(r)}remove(n){let e=this.savedLabelsSignal().filter(i=>i.id!==n);return this.persist(e)}toLayout(n){let e=qC(dl(n.sizePresetKey));return k(v({},e),{elements:n.elements})}exportAll(){gw($1,{savedLabels:this.savedLabelsSignal()})}async importFromFile(n){let e=await vw(n);if(!e||!Array.isArray(e.savedLabels))throw new Ul("Arquivo n\xE3o \xE9 uma exporta\xE7\xE3o v\xE1lida de etiquetas.");let i=e.savedLabels,r=new Set(this.savedLabelsSignal().map(s=>s.id)),o=i.filter(s=>!r.has(s.id));return this.persist([...this.savedLabelsSignal(),...o]),o.length}persist(n){let e=fs(_w,n);return e&&this.savedLabelsSignal.set(n),e}static \u0275fac=function(e){return new(e||t)};static \u0275prov=G({token:t,factory:t.\u0275fac,providedIn:"root"})};function G1(t){return t.toString(16).padStart(2,"0").toUpperCase()}function W1(t){let n="";for(let e=0;e<t.packedRows.length;e++)n+=G1(t.packedRows[e]);return n}function q1(t){let n=t.packedRows.length;return`^GFA,${n},${n},${t.bytesPerRow},${W1(t)}`}function yw(t){return["^XA",`^PW${t.widthDots}`,`^LL${t.heightDots}`,"^LH0,0","^FO0,0",q1(t),"^FS","^XZ"].join(`
`)}var Mf=class t{constructor(n,e){this.renderer=n;this.printerGateway=e}renderer;printerGateway;async execute(n,e){let i=await this.renderer.render(n),r=yw(i);await this.printerGateway.sendRaw(e,r)}static \u0275fac=function(e){return new(e||t)(A(Nu),A(ds))};static \u0275prov=G({token:t,factory:t.\u0275fac,providedIn:"root"})};var mr=class t{constructor(n){this.gateway=n}gateway;statusSignal=j("unknown");deviceSignal=j(null);status=this.statusSignal.asReadonly();device=this.deviceSignal.asReadonly();async refresh(){this.statusSignal.set("checking");try{let n=await this.gateway.detectDefaultPrinter();this.deviceSignal.set(n),this.statusSignal.set(n?"connected":"no-printer")}catch(n){this.deviceSignal.set(null),this.statusSignal.set(n instanceof ul?"agent-unavailable":"error")}}static \u0275fac=function(e){return new(e||t)(A(ds))};static \u0275prov=G({token:t,factory:t.\u0275fac,providedIn:"root"})};var Y1=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-text-field-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`textarea.cdk-textarea-autosize {
  resize: none;
}

textarea.cdk-textarea-autosize-measuring {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: auto !important;
  overflow: hidden !important;
}

textarea.cdk-textarea-autosize-measuring-firefox {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: 0 !important;
}

@keyframes cdk-text-field-autofill-start { /*!*/ }
@keyframes cdk-text-field-autofill-end { /*!*/ }
.cdk-text-field-autofill-monitored:-webkit-autofill {
  animation: cdk-text-field-autofill-start 0s 1ms;
}

.cdk-text-field-autofill-monitored:not(:-webkit-autofill) {
  animation: cdk-text-field-autofill-end 0s 1ms;
}
`],encapsulation:2})}return t})(),Z1={passive:!0},Sw=(()=>{class t{_platform=d(be);_ngZone=d(N);_renderer=d(Ke).createRenderer(null,null);_styleLoader=d(ze);_monitoredElements=new Map;monitor(e){if(!this._platform.isBrowser)return Ge;this._styleLoader.load(Y1);let i=$t(e),r=this._monitoredElements.get(i);if(r)return r.subject;let o=new E,s="cdk-text-field-autofilled",a=c=>{c.animationName==="cdk-text-field-autofill-start"&&!i.classList.contains(s)?(i.classList.add(s),this._ngZone.run(()=>o.next({target:c.target,isAutofilled:!0}))):c.animationName==="cdk-text-field-autofill-end"&&i.classList.contains(s)&&(i.classList.remove(s),this._ngZone.run(()=>o.next({target:c.target,isAutofilled:!1})))},l=this._ngZone.runOutsideAngular(()=>(i.classList.add("cdk-text-field-autofill-monitored"),this._renderer.listen(i,"animationstart",a,Z1)));return this._monitoredElements.set(i,{subject:o,unlisten:l}),o}stopMonitoring(e){let i=$t(e),r=this._monitoredElements.get(i);r&&(r.unlisten(),r.subject.complete(),i.classList.remove("cdk-text-field-autofill-monitored"),i.classList.remove("cdk-text-field-autofilled"),this._monitoredElements.delete(i))}ngOnDestroy(){this._monitoredElements.forEach((e,i)=>this.stopMonitoring(i))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();var Cw=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({})}return t})();var Dw=new b("MAT_INPUT_VALUE_ACCESSOR");var X1=["button","checkbox","file","hidden","image","radio","range","reset","submit"],K1=new b("MAT_INPUT_CONFIG"),Tf=(()=>{class t{_elementRef=d(O);_platform=d(be);ngControl=d(ks,{optional:!0,self:!0});_autofillMonitor=d(Sw);_ngZone=d(N);_formField=d(Tl,{optional:!0});_renderer=d(Re);_uid=d(xe).getId("mat-input-");_previousNativeValue;_inputValueAccessor;_signalBasedValueAccessor;_previousPlaceholder=null;_errorStateTracker;_config=d(K1,{optional:!0});_cleanupIosKeyup;_cleanupWebkitWheel;_isServer=!1;_isNativeSelect=!1;_isTextarea=!1;_isInFormField=!1;focused=!1;stateChanges=new E;controlType="mat-input";autofilled=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=sn(e),this.focused&&(this.focused=!1,this.stateChanges.next())}_disabled=!1;get id(){return this._id}set id(e){this._id=e||this._uid}_id;placeholder;name;get required(){return this._required??this.ngControl?.control?.hasValidator(yo.required)??!1}set required(e){this._required=sn(e)}_required;get type(){return this._type}set type(e){this._type=e||"text",this._validateType(),!this._isTextarea&&dv().has(this._type)&&(this._elementRef.nativeElement.type=this._type)}_type="text";get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}userAriaDescribedBy;get value(){return this._signalBasedValueAccessor?this._signalBasedValueAccessor.value():this._inputValueAccessor.value}set value(e){e!==this.value&&(this._signalBasedValueAccessor?this._signalBasedValueAccessor.value.set(e):this._inputValueAccessor.value=e,this.stateChanges.next())}get readonly(){return this._readonly}set readonly(e){this._readonly=sn(e)}_readonly=!1;disabledInteractive;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}_neverEmptyInputTypes=["date","datetime","datetime-local","month","time","week"].filter(e=>dv().has(e));constructor(){let e=d(Bl,{optional:!0}),i=d(jl,{optional:!0}),r=d(Ef),o=d(Dw,{optional:!0,self:!0}),s=d(Cf,{optional:!0,self:!0}),a=this._elementRef.nativeElement,l=a.nodeName.toLowerCase();o?Zt(o.value)?this._signalBasedValueAccessor=o:this._inputValueAccessor=o:this._inputValueAccessor=a,this._previousNativeValue=this.value,this.id=this.id,this._platform.IOS&&this._ngZone.runOutsideAngular(()=>{this._cleanupIosKeyup=this._renderer.listen(a,"keyup",this._iOSKeyupListener)}),this._errorStateTracker=new As(r,s||this.ngControl,i,e,this.stateChanges),this._isServer=!this._platform.isBrowser,this._isNativeSelect=l==="select",this._isTextarea=l==="textarea",this._isInFormField=!!this._formField,this.disabledInteractive=this._config?.disabledInteractive||!1,this._isNativeSelect&&(this.controlType=a.multiple?"mat-native-select-multiple":"mat-native-select"),this._signalBasedValueAccessor&&ut(()=>{this._signalBasedValueAccessor.value(),this.stateChanges.next()})}ngAfterViewInit(){this._platform.isBrowser&&this._autofillMonitor.monitor(this._elementRef.nativeElement).subscribe(e=>{this.autofilled=e.isAutofilled,this.stateChanges.next()})}ngOnChanges(){this.stateChanges.next()}ngOnDestroy(){this.stateChanges.complete(),this._platform.isBrowser&&this._autofillMonitor.stopMonitoring(this._elementRef.nativeElement),this._cleanupIosKeyup?.(),this._cleanupWebkitWheel?.()}ngDoCheck(){this.ngControl&&(this.updateErrorState(),this.ngControl.disabled!==null&&this.ngControl.disabled!==this.disabled&&(this.disabled=this.ngControl.disabled,this.stateChanges.next())),this._dirtyCheckNativeValue(),this._dirtyCheckPlaceholder()}focus(e){this._elementRef.nativeElement.focus(e)}updateErrorState(){this._errorStateTracker.updateErrorState()}_focusChanged(e){if(e!==this.focused){if(!this._isNativeSelect&&e&&this.disabled&&this.disabledInteractive){let i=this._elementRef.nativeElement;i.type==="number"?(i.type="text",i.setSelectionRange(0,0),i.type="number"):i.setSelectionRange(0,0)}this.focused=e,this.stateChanges.next()}}_onInput(){}_dirtyCheckNativeValue(){let e=this._elementRef.nativeElement.value;this._previousNativeValue!==e&&(this._previousNativeValue=e,this.stateChanges.next())}_dirtyCheckPlaceholder(){let e=this._getPlaceholder();if(e!==this._previousPlaceholder){let i=this._elementRef.nativeElement;this._previousPlaceholder=e,e?i.setAttribute("placeholder",e):i.removeAttribute("placeholder")}}_getPlaceholder(){return this.placeholder||null}_validateType(){X1.indexOf(this._type)>-1}_isNeverEmpty(){return this._neverEmptyInputTypes.indexOf(this._type)>-1}_isBadInput(){let e=this._elementRef.nativeElement.validity;return e&&e.badInput}get empty(){return!this._isNeverEmpty()&&!this._elementRef.nativeElement.value&&!this._isBadInput()&&!this.autofilled}get shouldLabelFloat(){if(this._isNativeSelect){let e=this._elementRef.nativeElement,i=e.options[0];return this.focused||e.multiple||!this.empty||!!(e.selectedIndex>-1&&i&&i.label)}else return this.focused&&!this.disabled||!this.empty}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(){this.focused||this.focus()}_isInlineSelect(){let e=this._elementRef.nativeElement;return this._isNativeSelect&&(e.multiple||e.size>1)}_iOSKeyupListener=e=>{let i=e.target;!i.value&&i.selectionStart===0&&i.selectionEnd===0&&(i.setSelectionRange(1,1),i.setSelectionRange(0,0))};_getReadonlyAttribute(){return this._isNativeSelect?null:this.readonly||this.disabled&&this.disabledInteractive?"true":null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,selectors:[["input","matInput",""],["textarea","matInput",""],["select","matNativeControl",""],["input","matNativeControl",""],["textarea","matNativeControl",""]],hostAttrs:[1,"mat-mdc-input-element"],hostVars:21,hostBindings:function(i,r){i&1&&D("focus",function(){return r._focusChanged(!0)})("blur",function(){return r._focusChanged(!1)})("input",function(){return r._onInput()}),i&2&&(Et("id",r.id)("disabled",r.disabled&&!r.disabledInteractive)("required",r.required),re("name",r.name||null)("readonly",r._getReadonlyAttribute())("aria-disabled",r.disabled&&r.disabledInteractive?"true":null)("aria-invalid",r.empty&&r.required?null:r.errorState)("aria-required",r.required)("id",r.id),z("mat-input-server",r._isServer)("mat-mdc-form-field-textarea-control",r._isInFormField&&r._isTextarea)("mat-mdc-form-field-input-control",r._isInFormField)("mat-mdc-input-disabled-interactive",r.disabledInteractive)("mdc-text-field__input",r._isInFormField)("mat-mdc-native-select-inline",r._isInlineSelect()))},inputs:{disabled:"disabled",id:"id",placeholder:"placeholder",name:"name",required:"required",type:"type",errorStateMatcher:"errorStateMatcher",userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],value:"value",readonly:"readonly",disabledInteractive:[2,"disabledInteractive","disabledInteractive",P]},exportAs:["matInput"],features:[Be([{provide:Ml,useExisting:t}]),at]})}return t})(),Nf=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({imports:[Tn,Tn,Cw,ve]})}return t})();function Q1(t,n){t&1&&(m(0,"mat-error"),y(1),g()),t&2&&(_(),Te(n))}function J1(t,n){t&1&&(m(0,"mat-error"),y(1),g()),t&2&&(_(),Te(n))}var Os=class t{dialogRef=d(Ds);settings=d(er);connection=d(mr);hostDraft=j(this.settings.config().host);portDraft=j(this.settings.config().port?.toString()??"");hostError=Q(()=>this.hostDraft().trim().length===0?"Informe um IP ou nome de host.":null);portError=Q(()=>{let n=this.portDraft().trim();if(n.length===0)return null;let e=Number(n);return Number.isInteger(e)&&e>0&&e<=65535?null:"Porta inv\xE1lida (use um n\xFAmero entre 1 e 65535, ou deixe em branco)."});canSave=Q(()=>!this.hostError()&&!this.portError());onHostInput(n){this.hostDraft.set(n)}onPortInput(n){this.portDraft.set(n)}onSave(){if(!this.canSave())return;let n=this.portDraft().trim();this.settings.update({host:this.hostDraft().trim(),port:n.length===0?null:Number(n)}),this.connection.refresh(),this.dialogRef.close()}onReset(){this.settings.reset(),this.hostDraft.set(Ji.host),this.portDraft.set(Ji.port?.toString()??""),this.connection.refresh(),this.dialogRef.close()}onCancel(){this.dialogRef.close()}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=T({type:t,selectors:[["app-printer-connection-settings-dialog"]],decls:30,vars:5,consts:[["mat-dialog-title",""],[1,"dialog-content"],[1,"hint"],["appearance","outline",1,"full-width"],["matInput","","type","text","placeholder","127.0.0.1",3,"input","value"],["matInput","","type","number","min","1","max","65535","placeholder","9100",3,"input","value"],["align","end"],["mat-button","","type","button",3,"click"],["mat-flat-button","","color","primary","type","button",3,"click","disabled"]],template:function(e,i){if(e&1&&(m(0,"h2",0),y(1,"Endere\xE7o da impressora"),g(),m(2,"mat-dialog-content",1)(3,"p",2),y(4," Deixe "),m(5,"strong"),y(6,"127.0.0.1"),g(),y(7," e a porta "),m(8,"strong"),y(9,"9100"),g(),y(10," se o Zebra Browser Print roda neste computador. Pra alcan\xE7ar uma impressora em outra m\xE1quina da rede (por exemplo, atr\xE1s de um relay), informe o IP/host e a porta dela. "),g(),m(11,"p",2),y(12," A conex\xE3o \xE9 sempre HTTP. Se o app estiver aberto por HTTPS, o navegador bloqueia a chamada sem avisar \u2014 abra o app por HTTP (pelo IP da rede local, por exemplo) pra conseguir imprimir. "),g(),m(13,"mat-form-field",3)(14,"mat-label"),y(15,"IP ou host"),g(),m(16,"input",4),D("input",function(o){return i.onHostInput(o.target.value)}),g(),V(17,Q1,2,1,"mat-error"),g(),m(18,"mat-form-field",3)(19,"mat-label"),y(20,"Porta"),g(),m(21,"input",5),D("input",function(o){return i.onPortInput(o.target.value)}),g(),V(22,J1,2,1,"mat-error"),g()(),m(23,"mat-dialog-actions",6)(24,"button",7),D("click",function(){return i.onReset()}),y(25,"Restaurar padr\xE3o"),g(),m(26,"button",7),D("click",function(){return i.onCancel()}),y(27,"Cancelar"),g(),m(28,"button",8),D("click",function(){return i.onSave()}),y(29," Salvar e verificar "),g()()),e&2){let r,o;_(16),U("value",i.hostDraft()),_(),B((r=i.hostError())?17:-1,r),_(4),U("value",i.portDraft()),_(),B((o=i.portError())?22:-1,o),_(6),U("disabled",!i.canSave())}},dependencies:[Mn,lr,mE,uE,hE,fE,Tn,cr,Di,yv,Nf,Tf],styles:[".dialog-content[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:.5rem;min-width:320px}.hint[_ngcontent-%COMP%]{font-size:.8125rem;color:var(--%NS%mat-sys-on-surface-variant, #616161);margin:0 0 .5rem}.full-width[_ngcontent-%COMP%]{width:100%}"]})};var eL=["tooltip"],tL=20;var nL=new b("mat-tooltip-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(R);return()=>Si(t,{scrollThrottle:tL})}}),iL=new b("mat-tooltip-default-options",{providedIn:"root",factory:()=>({showDelay:0,hideDelay:0,touchendHideDelay:1500})});var ww="tooltip-panel",rL={passive:!0},oL=8,sL=8,aL=24,lL=200,zl=(()=>{class t{_elementRef=d(O);_ngZone=d(N);_platform=d(be);_ariaDescriber=d(qD);_focusMonitor=d(rn);_dir=d(ct);_injector=d(R);_viewContainerRef=d(bt);_mediaMatcher=d(gs);_document=d(L);_renderer=d(Re);_animationsDisabled=Ce();_defaultOptions=d(iL,{optional:!0});_overlayRef=null;_tooltipInstance=null;_overlayPanelClass;_portal;_position="below";_positionAtOrigin=!1;_disabled=!1;_tooltipClass;_viewInitialized=!1;_pointerExitEventsInitialized=!1;_tooltipComponent=xw;_viewportMargin=8;_currentPosition;_cssClassPrefix="mat-mdc";_ariaDescriptionPending=!1;_dirSubscribed=!1;get position(){return this._position}set position(e){e!==this._position&&(this._position=e,this._overlayRef&&(this._updatePosition(this._overlayRef),this._tooltipInstance?.show(0),this._overlayRef.updatePosition()))}get positionAtOrigin(){return this._positionAtOrigin}set positionAtOrigin(e){this._positionAtOrigin=sn(e),this._detach(),this._overlayRef=null}get disabled(){return this._disabled}set disabled(e){let i=sn(e);this._disabled!==i&&(this._disabled=i,i?this.hide(0):this._setupPointerEnterEventsIfNeeded(),this._syncAriaDescription(this.message))}get showDelay(){return this._showDelay}set showDelay(e){this._showDelay=bi(e)}_showDelay;get hideDelay(){return this._hideDelay}set hideDelay(e){this._hideDelay=bi(e),this._tooltipInstance&&(this._tooltipInstance._mouseLeaveHideDelay=this._hideDelay)}_hideDelay;touchGestures="auto";get message(){return this._message}set message(e){let i=this._message;this._message=e!=null?String(e).trim():"",!this._message&&this._isTooltipVisible()?this.hide(0):(this._setupPointerEnterEventsIfNeeded(),this._updateTooltipMessage()),this._syncAriaDescription(i)}_message="";get tooltipClass(){return this._tooltipClass}set tooltipClass(e){this._tooltipClass=e,this._tooltipInstance&&this._setTooltipClass(this._tooltipClass)}_eventCleanups=[];_touchstartTimeout=null;_destroyed=new E;_isDestroyed=!1;constructor(){let e=this._defaultOptions;e&&(this._showDelay=e.showDelay,this._hideDelay=e.hideDelay,e.position&&(this.position=e.position),e.positionAtOrigin&&(this.positionAtOrigin=e.positionAtOrigin),e.touchGestures&&(this.touchGestures=e.touchGestures),e.tooltipClass&&(this.tooltipClass=e.tooltipClass)),this._viewportMargin=oL}ngAfterViewInit(){this._viewInitialized=!0,this._setupPointerEnterEventsIfNeeded(),this._focusMonitor.monitor(this._elementRef).pipe(ye(this._destroyed)).subscribe(e=>{e?e==="keyboard"&&this._ngZone.run(()=>this.show()):this._ngZone.run(()=>this.hide(0))})}ngOnDestroy(){let e=this._elementRef.nativeElement;this._touchstartTimeout&&clearTimeout(this._touchstartTimeout),this._overlayRef&&(this._overlayRef.dispose(),this._tooltipInstance=null),this._eventCleanups.forEach(i=>i()),this._eventCleanups.length=0,this._destroyed.next(),this._destroyed.complete(),this._isDestroyed=!0,this._ariaDescriber.removeDescription(e,this.message,"tooltip"),this._focusMonitor.stopMonitoring(e)}show(e=this.showDelay,i){if(this.disabled||!this.message||this._isTooltipVisible()){this._tooltipInstance?._cancelPendingAnimations();return}let r=this._createOverlay(i);this._detach(),this._portal=this._portal||new Cn(this._tooltipComponent,this._viewContainerRef);let o=this._tooltipInstance=r.attach(this._portal).instance;o._triggerElement=this._elementRef.nativeElement,o._mouseLeaveHideDelay=this._hideDelay,o.afterHidden().pipe(ye(this._destroyed)).subscribe(()=>this._detach()),this._setTooltipClass(this._tooltipClass),this._updateTooltipMessage(),o.show(e)}hide(e=this.hideDelay){let i=this._tooltipInstance;i&&(i.isVisible()?i.hide(e):(i._cancelPendingAnimations(),this._detach()))}toggle(e){this._isTooltipVisible()?this.hide():this.show(void 0,e)}_isTooltipVisible(){return!!this._tooltipInstance&&this._tooltipInstance.isVisible()}_createOverlay(e){if(this._overlayRef){let s=this._overlayRef.getConfig().positionStrategy;if((!this.positionAtOrigin||!e)&&s._origin instanceof O)return this._overlayRef;this._detach()}let i=this._injector.get(ir).getAncestorScrollContainers(this._elementRef),r=`${this._cssClassPrefix}-${ww}`,o=fo(this._injector,this.positionAtOrigin?e||this._elementRef:this._elementRef).withTransformOriginOn(`.${this._cssClassPrefix}-tooltip`).withFlexibleDimensions(!1).withViewportMargin(this._viewportMargin).withScrollableContainers(i).withPopoverLocation("global");return o.positionChanges.pipe(ye(this._destroyed)).subscribe(s=>{this._updateCurrentPositionClass(s.connectionPair),this._tooltipInstance&&s.scrollableViewProperties.isOverlayClipped&&this._tooltipInstance.isVisible()&&this._ngZone.run(()=>this.hide(0))}),this._overlayRef=In(this._injector,{direction:this._dir,positionStrategy:o,panelClass:this._overlayPanelClass?[...this._overlayPanelClass,r]:r,scrollStrategy:this._injector.get(nL)(),disableAnimations:this._animationsDisabled,eventPredicate:this._overlayEventPredicate}),this._updatePosition(this._overlayRef),this._overlayRef.detachments().pipe(ye(this._destroyed)).subscribe(()=>this._detach()),this._overlayRef.outsidePointerEvents().pipe(ye(this._destroyed)).subscribe(()=>this._tooltipInstance?._handleBodyInteraction()),this._overlayRef.keydownEvents().pipe(ye(this._destroyed)).subscribe(s=>{s.preventDefault(),s.stopPropagation(),this._ngZone.run(()=>this.hide(0))}),this._defaultOptions?.disableTooltipInteractivity&&this._overlayRef.addPanelClass(`${this._cssClassPrefix}-tooltip-panel-non-interactive`),this._dirSubscribed||(this._dirSubscribed=!0,this._dir.change.pipe(ye(this._destroyed)).subscribe(()=>{this._overlayRef&&this._updatePosition(this._overlayRef)})),this._overlayRef}_detach(){this._overlayRef&&this._overlayRef.hasAttached()&&this._overlayRef.detach(),this._tooltipInstance=null}_updatePosition(e){let i=e.getConfig().positionStrategy,r=this._getOrigin(),o=this._getOverlayPosition();i.withPositions([this._addOffset(v(v({},r.main),o.main)),this._addOffset(v(v({},r.fallback),o.fallback))])}_addOffset(e){let i=sL,r=!this._dir||this._dir.value=="ltr";return e.originY==="top"?e.offsetY=-i:e.originY==="bottom"?e.offsetY=i:e.originX==="start"?e.offsetX=r?-i:i:e.originX==="end"&&(e.offsetX=r?i:-i),e}_getOrigin(){let e=!this._dir||this._dir.value=="ltr",i=this.position,r;i=="above"||i=="below"?r={originX:"center",originY:i=="above"?"top":"bottom"}:i=="before"||i=="left"&&e||i=="right"&&!e?r={originX:"start",originY:"center"}:(i=="after"||i=="right"&&e||i=="left"&&!e)&&(r={originX:"end",originY:"center"});let{x:o,y:s}=this._invertPosition(r.originX,r.originY);return{main:r,fallback:{originX:o,originY:s}}}_getOverlayPosition(){let e=!this._dir||this._dir.value=="ltr",i=this.position,r;i=="above"?r={overlayX:"center",overlayY:"bottom"}:i=="below"?r={overlayX:"center",overlayY:"top"}:i=="before"||i=="left"&&e||i=="right"&&!e?r={overlayX:"end",overlayY:"center"}:(i=="after"||i=="right"&&e||i=="left"&&!e)&&(r={overlayX:"start",overlayY:"center"});let{x:o,y:s}=this._invertPosition(r.overlayX,r.overlayY);return{main:r,fallback:{overlayX:o,overlayY:s}}}_updateTooltipMessage(){this._tooltipInstance&&(this._tooltipInstance.message=this.message,this._tooltipInstance._markForCheck(),Qe(()=>{this._tooltipInstance&&this._overlayRef.updatePosition()},{injector:this._injector}))}_setTooltipClass(e){this._tooltipInstance&&(this._tooltipInstance.tooltipClass=e instanceof Set?Array.from(e):e,this._tooltipInstance._markForCheck())}_invertPosition(e,i){return this.position==="above"||this.position==="below"?i==="top"?i="bottom":i==="bottom"&&(i="top"):e==="end"?e="start":e==="start"&&(e="end"),{x:e,y:i}}_updateCurrentPositionClass(e){let{overlayY:i,originX:r,originY:o}=e,s;if(i==="center"?this._dir&&this._dir.value==="rtl"?s=r==="end"?"left":"right":s=r==="start"?"left":"right":s=i==="bottom"&&o==="top"?"above":"below",s!==this._currentPosition){let a=this._overlayRef;if(a){let l=`${this._cssClassPrefix}-${ww}-`;a.removePanelClass(l+this._currentPosition),a.addPanelClass(l+s)}this._currentPosition=s}}_setupPointerEnterEventsIfNeeded(){this._disabled||!this.message||!this._viewInitialized||this._eventCleanups.length||(this._isTouchPlatform()?this.touchGestures!=="off"&&(this._disableNativeGesturesIfNecessary(),this._addListener("touchstart",e=>{let i=e.targetTouches?.[0],r=i?{x:i.clientX,y:i.clientY}:void 0;this._setupPointerExitEventsIfNeeded(),this._touchstartTimeout&&clearTimeout(this._touchstartTimeout);let o=500;this._touchstartTimeout=setTimeout(()=>{this._touchstartTimeout=null,this.show(void 0,r)},this._defaultOptions?.touchLongPressShowDelay??o)})):this._addListener("mouseenter",e=>{this._setupPointerExitEventsIfNeeded();let i;e.x!==void 0&&e.y!==void 0&&(i=e),this.show(void 0,i)}))}_setupPointerExitEventsIfNeeded(){if(!this._pointerExitEventsInitialized){if(this._pointerExitEventsInitialized=!0,!this._isTouchPlatform())this._addListener("mouseleave",e=>{let i=e.relatedTarget;(!i||!this._overlayRef?.overlayElement.contains(i))&&this.hide()}),this._addListener("wheel",e=>{if(this._isTooltipVisible()){let i=this._document.elementFromPoint(e.clientX,e.clientY),r=this._elementRef.nativeElement;i!==r&&!r.contains(i)&&this.hide()}});else if(this.touchGestures!=="off"){this._disableNativeGesturesIfNecessary();let e=()=>{this._touchstartTimeout&&clearTimeout(this._touchstartTimeout),this.hide(this._defaultOptions?.touchendHideDelay)};this._addListener("touchend",e),this._addListener("touchcancel",e)}}}_addListener(e,i){this._eventCleanups.push(this._renderer.listen(this._elementRef.nativeElement,e,i,rL))}_isTouchPlatform(){let e=this._defaultOptions?.detectHoverCapability;return typeof e=="function"?!e():this._platform.IOS||this._platform.ANDROID?!0:this._platform.isBrowser?!!e&&this._mediaMatcher.matchMedia("(any-hover: none)").matches:!1}_disableNativeGesturesIfNecessary(){let e=this.touchGestures;if(e!=="off"){let i=this._elementRef.nativeElement,r=i.style;(e==="on"||i.nodeName!=="INPUT"&&i.nodeName!=="TEXTAREA")&&(r.userSelect=r.msUserSelect=r.webkitUserSelect=r.MozUserSelect="none"),(e==="on"||!i.draggable)&&(r.webkitUserDrag="none"),r.touchAction="none",r.webkitTapHighlightColor="transparent"}}_syncAriaDescription(e){this._ariaDescriptionPending||(this._ariaDescriptionPending=!0,this._ariaDescriber.removeDescription(this._elementRef.nativeElement,e,"tooltip"),this._isDestroyed||Qe({write:()=>{this._ariaDescriptionPending=!1,this.message&&!this.disabled&&this._ariaDescriber.describe(this._elementRef.nativeElement,this.message,"tooltip")}},{injector:this._injector}))}_overlayEventPredicate=e=>e.type==="keydown"?this._isTooltipVisible()&&e.keyCode===27&&!$e(e):!0;static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,selectors:[["","matTooltip",""]],hostAttrs:[1,"mat-mdc-tooltip-trigger"],hostVars:2,hostBindings:function(i,r){i&2&&z("mat-mdc-tooltip-disabled",r.disabled)},inputs:{position:[0,"matTooltipPosition","position"],positionAtOrigin:[0,"matTooltipPositionAtOrigin","positionAtOrigin"],disabled:[0,"matTooltipDisabled","disabled"],showDelay:[0,"matTooltipShowDelay","showDelay"],hideDelay:[0,"matTooltipHideDelay","hideDelay"],touchGestures:[0,"matTooltipTouchGestures","touchGestures"],message:[0,"matTooltip","message"],tooltipClass:[0,"matTooltipClass","tooltipClass"]},exportAs:["matTooltip"]})}return t})(),xw=(()=>{class t{_changeDetectorRef=d(Oe);_elementRef=d(O);_isMultiline=!1;message;tooltipClass;_showTimeoutId;_hideTimeoutId;_triggerElement;_mouseLeaveHideDelay;_animationsDisabled=Ce();_tooltip;_closeOnInteraction=!1;_isVisible=!1;_onHide=new E;_showAnimation="mat-mdc-tooltip-show";_hideAnimation="mat-mdc-tooltip-hide";show(e){this._hideTimeoutId!=null&&clearTimeout(this._hideTimeoutId),this._showTimeoutId=setTimeout(()=>{this._toggleVisibility(!0),this._showTimeoutId=void 0},e)}hide(e){this._showTimeoutId!=null&&clearTimeout(this._showTimeoutId),this._hideTimeoutId=setTimeout(()=>{this._toggleVisibility(!1),this._hideTimeoutId=void 0},e)}afterHidden(){return this._onHide}isVisible(){return this._isVisible}ngOnDestroy(){this._cancelPendingAnimations(),this._onHide.complete(),this._triggerElement=null}_handleBodyInteraction(){this._closeOnInteraction&&this.hide(0)}_markForCheck(){this._changeDetectorRef.markForCheck()}_handleMouseLeave({relatedTarget:e}){(!e||!this._triggerElement.contains(e))&&(this.isVisible()?this.hide(this._mouseLeaveHideDelay):this._finalizeAnimation(!1))}_onShow(){this._isMultiline=this._isTooltipMultiline(),this._markForCheck()}_isTooltipMultiline(){let e=this._elementRef.nativeElement.getBoundingClientRect();return e.height>aL&&e.width>=lL}_handleAnimationEnd({animationName:e}){(e===this._showAnimation||e===this._hideAnimation)&&this._finalizeAnimation(e===this._showAnimation)}_cancelPendingAnimations(){this._showTimeoutId!=null&&clearTimeout(this._showTimeoutId),this._hideTimeoutId!=null&&clearTimeout(this._hideTimeoutId),this._showTimeoutId=this._hideTimeoutId=void 0}_finalizeAnimation(e){e?this._closeOnInteraction=!0:this.isVisible()||this._onHide.next()}_toggleVisibility(e){let i=this._tooltip.nativeElement,r=this._showAnimation,o=this._hideAnimation;if(i.classList.remove(e?o:r),i.classList.add(e?r:o),this._isVisible!==e&&(this._isVisible=e,this._changeDetectorRef.markForCheck()),e&&!this._animationsDisabled&&typeof getComputedStyle=="function"){let s=getComputedStyle(i);(s.getPropertyValue("animation-duration")==="0s"||s.getPropertyValue("animation-name")==="none")&&(this._animationsDisabled=!0)}e&&this._onShow(),this._animationsDisabled&&(i.classList.add("_mat-animation-noopable"),this._finalizeAnimation(e))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-tooltip-component"]],viewQuery:function(i,r){if(i&1&&Ue(eL,7),i&2){let o;ce(o=de())&&(r._tooltip=o.first)}},hostAttrs:["aria-hidden","true"],hostBindings:function(i,r){i&1&&D("mouseleave",function(s){return r._handleMouseLeave(s)})},decls:4,vars:5,consts:[["tooltip",""],[1,"mdc-tooltip","mat-mdc-tooltip",3,"animationend"],[1,"mat-mdc-tooltip-surface","mdc-tooltip__surface"]],template:function(i,r){i&1&&(lt(0,"div",1,0),Ko("animationend",function(s){return r._handleAnimationEnd(s)}),lt(2,"div",2),y(3),_t()()),i&2&&(wt(r.tooltipClass),z("mdc-tooltip--multiline",r._isMultiline),_(3),Te(r.message))},styles:[`.mat-mdc-tooltip {
  position: relative;
  transform: scale(0);
  display: inline-flex;
}
.mat-mdc-tooltip::before {
  content: "";
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  z-index: -1;
  position: absolute;
}
.mat-mdc-tooltip-panel-below .mat-mdc-tooltip::before {
  top: -8px;
}
.mat-mdc-tooltip-panel-above .mat-mdc-tooltip::before {
  bottom: -8px;
}
.mat-mdc-tooltip-panel-right .mat-mdc-tooltip::before {
  left: -8px;
}
.mat-mdc-tooltip-panel-left .mat-mdc-tooltip::before {
  right: -8px;
}
.mat-mdc-tooltip._mat-animation-noopable {
  animation: none;
  transform: scale(1);
}

.mat-mdc-tooltip-surface {
  word-break: normal;
  overflow-wrap: anywhere;
  padding: 4px 8px;
  min-width: 40px;
  max-width: 200px;
  min-height: 24px;
  max-height: 40vh;
  box-sizing: border-box;
  overflow: hidden;
  text-align: center;
  will-change: transform, opacity;
  background-color: var(--%NS%mat-tooltip-container-color, var(--%NS%mat-sys-inverse-surface));
  color: var(--%NS%mat-tooltip-supporting-text-color, var(--%NS%mat-sys-inverse-on-surface));
  border-radius: var(--%NS%mat-tooltip-container-shape, var(--%NS%mat-sys-corner-extra-small));
  font-family: var(--%NS%mat-tooltip-supporting-text-font, var(--%NS%mat-sys-body-small-font));
  font-size: var(--%NS%mat-tooltip-supporting-text-size, var(--%NS%mat-sys-body-small-size));
  font-weight: var(--%NS%mat-tooltip-supporting-text-weight, var(--%NS%mat-sys-body-small-weight));
  line-height: var(--%NS%mat-tooltip-supporting-text-line-height, var(--%NS%mat-sys-body-small-line-height));
  letter-spacing: var(--%NS%mat-tooltip-supporting-text-tracking, var(--%NS%mat-sys-body-small-tracking));
}
.mat-mdc-tooltip-surface::before {
  position: absolute;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  border: 1px solid transparent;
  border-radius: inherit;
  content: "";
  pointer-events: none;
}
.mdc-tooltip--multiline .mat-mdc-tooltip-surface {
  text-align: left;
}
[dir=rtl] .mdc-tooltip--multiline .mat-mdc-tooltip-surface {
  text-align: right;
}

.mat-mdc-tooltip-panel {
  line-height: normal;
}
.mat-mdc-tooltip-panel.mat-mdc-tooltip-panel-non-interactive {
  pointer-events: none;
}

@keyframes mat-mdc-tooltip-show {
  0% {
    opacity: 0;
    transform: scale(0.8);
  }
  100% {
    opacity: 1;
    transform: scale(1);
  }
}
@keyframes mat-mdc-tooltip-hide {
  0% {
    opacity: 1;
    transform: scale(1);
  }
  100% {
    opacity: 0;
    transform: scale(0.8);
  }
}
.mat-mdc-tooltip-show {
  animation: mat-mdc-tooltip-show 150ms cubic-bezier(0, 0, 0.2, 1) forwards;
}

.mat-mdc-tooltip-hide {
  animation: mat-mdc-tooltip-hide 75ms cubic-bezier(0.4, 0, 1, 1) forwards;
}
`],encapsulation:2})}return t})();var kf=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({imports:[bl,on,ve,_i]})}return t})();function cL(t,n){if(t&1){let e=Le();m(0,"button",2),D("click",function(){ee(e);let r=w();return te(r.onOpenSettings())}),m(1,"mat-icon"),y(2),g(),m(3,"span",3),y(4),g(),m(5,"mat-icon",4),y(6,"settings"),g()()}if(t&2){let e=w();wt(e.presentation().cssClass),re("aria-label","Impressora: "+e.statusText()+". Tocar para configurar o endere\xE7o."),_(2),Te(e.presentation().icon),_(2),Te(e.compactText())}}function dL(t,n){t&1&&(m(0,"span",8),y(1),g()),t&2&&(_(),Te(n))}function uL(t,n){if(t&1){let e=Le();m(0,"button",12),D("click",function(){ee(e);let r=w(2);return te(r.onRetry())}),m(1,"mat-icon"),y(2,"refresh"),g()()}}function fL(t,n){t&1&&(m(0,"a",13),y(1," Instalar Browser Print "),g())}function hL(t,n){if(t&1&&V(0,fL,2,0,"a",13),t&2){let e=w(2);B(e.settings.isCustom()?-1:0)}}function mL(t,n){if(t&1){let e=Le();m(0,"div",5)(1,"div",6)(2,"mat-icon"),y(3),g(),m(4,"span",7),y(5),g(),V(6,dL,2,1,"span",8),g(),m(7,"div",9),V(8,uL,3,0,"button",10),m(9,"button",11),D("click",function(){ee(e);let r=w();return te(r.onOpenSettings())}),m(10,"mat-icon"),y(11,"settings"),g()(),V(12,hL,1,1),g()()}if(t&2){let e,i=w();wt(i.presentation().cssClass),_(3),Te(i.presentation().icon),_(2),Te(i.statusText()),_(),B((e=i.customAddressLabel())?6:-1,e),_(2),B(i.store.status()!=="connected"&&i.store.status()!=="checking"?8:-1),_(4),B(i.store.status()==="agent-unavailable"?12:-1)}}var Mw={unknown:{icon:"help_outline",label:"Verificando impressora\u2026",cssClass:"is-checking"},checking:{icon:"sync",label:"Verificando impressora\u2026",cssClass:"is-checking"},connected:{icon:"check_circle",label:"",cssClass:"is-connected"},"no-printer":{icon:"print_disabled",label:"Nenhuma impressora padr\xE3o configurada no Browser Print.",cssClass:"is-warning"},"agent-unavailable":{icon:"usb_off",label:"Zebra Browser Print n\xE3o encontrado neste computador.",cssClass:"is-error"},error:{icon:"error",label:"Erro ao verificar a impressora.",cssClass:"is-error"}},Af=class t{compact=yn(!1,{transform:P});store=d(mr);settings=d(er);dialog=d(Es);presentation=Q(()=>Mw[this.store.status()]??Mw.error);statusText=Q(()=>{let n=this.store.device();return this.store.status()==="connected"&&n?`Impressora pronta: ${n.name}`:this.store.status()==="agent-unavailable"&&this.settings.isCustom()?`Sem resposta do servidor de impress\xE3o em ${this.addressLabel()}.`:this.presentation().label});compactText=Q(()=>{let n=this.store.device();switch(this.store.status()){case"connected":return n?`${n.name} \xB7 ${this.addressLabel()}`:"Impressora pronta";case"checking":case"unknown":return"Verificando impressora\u2026";case"no-printer":return"Sem impressora padr\xE3o";default:return this.settings.isCustom()?`Sem resposta \xB7 ${this.addressLabel()}`:"Apontar a impressora"}});addressLabel=Q(()=>{let{host:n,port:e}=this.settings.config();return e?`${n}:${e}`:n});customAddressLabel=Q(()=>this.settings.isCustom()?this.addressLabel():null);ngOnInit(){this.store.refresh()}onRetry(){this.store.refresh()}onOpenSettings(){this.dialog.open(Os)}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=T({type:t,selectors:[["app-printer-status-badge"]],inputs:{compact:[1,"compact"]},decls:2,vars:1,consts:[["type","button",1,"status-chip",3,"class"],[1,"status-badge",3,"class"],["type","button",1,"status-chip",3,"click"],[1,"chip-text"],[1,"chip-caret"],[1,"status-badge"],[1,"status-main"],[1,"status-text"],["matTooltip","Endere\xE7o configurado do agente Zebra Browser Print",1,"custom-address"],[1,"status-actions"],["mat-icon-button","","type","button","matTooltip","Tentar de novo"],["mat-icon-button","","type","button","matTooltip","Configurar IP/porta da impressora na rede",3,"click"],["mat-icon-button","","type","button","matTooltip","Tentar de novo",3,"click"],["href","https://developer.zebra.com/products/printers/browser-print","target","_blank","rel","noopener","matTooltip","Baixe e instale o Zebra Browser Print no site oficial da Zebra",1,"ssl-hint"]],template:function(e,i){e&1&&V(0,cL,7,5,"button",0)(1,mL,13,7,"div",1),e&2&&B(i.compact()?0:1)},dependencies:[Mn,bo,Zn,Yn,kf,zl],styles:["[_nghost-%COMP%]{min-width:0}.status-chip[_ngcontent-%COMP%]{display:inline-flex;align-items:center;gap:.375rem;max-width:100%;min-height:40px;padding:0 .75rem;border:0;border-radius:20px;font:inherit;font-size:.8125rem;cursor:pointer;background:color-mix(in srgb,currentColor 12%,transparent)}.status-chip[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:1.125rem;width:1.125rem;height:1.125rem;flex-shrink:0}.status-chip[_ngcontent-%COMP%]   .chip-text[_ngcontent-%COMP%]{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.status-chip[_ngcontent-%COMP%]   .chip-caret[_ngcontent-%COMP%]{opacity:.7}.status-chip.is-connected[_ngcontent-%COMP%]{color:var(--%NS%mat-sys-primary, #2e7d32)}.status-chip.is-warning[_ngcontent-%COMP%]{color:#b26a00}.status-chip.is-error[_ngcontent-%COMP%]{color:var(--%NS%mat-sys-error, #c62828)}.status-chip.is-checking[_ngcontent-%COMP%]{color:var(--%NS%mat-sys-on-surface-variant, #616161)}.status-badge[_ngcontent-%COMP%]{display:flex;flex-wrap:wrap;align-items:center;column-gap:.75rem;row-gap:.375rem;padding:.5rem .875rem;border-radius:14px;font-size:.875rem;background:color-mix(in srgb,currentColor 10%,transparent)}.status-badge[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:1.25rem;width:1.25rem;height:1.25rem;flex-shrink:0}.status-badge.is-connected[_ngcontent-%COMP%]{color:var(--%NS%mat-sys-primary, #2e7d32)}.status-badge.is-warning[_ngcontent-%COMP%]{color:#b26a00}.status-badge.is-error[_ngcontent-%COMP%]{color:var(--%NS%mat-sys-error, #c62828)}.status-badge.is-checking[_ngcontent-%COMP%]{color:var(--%NS%mat-sys-on-surface-variant, #616161)}.status-main[_ngcontent-%COMP%]{display:flex;align-items:center;gap:.5rem;min-width:0}.status-text[_ngcontent-%COMP%]{min-width:0}.status-actions[_ngcontent-%COMP%]{display:flex;flex-wrap:wrap;align-items:center;column-gap:.75rem;row-gap:.25rem}.ssl-hint[_ngcontent-%COMP%]{font-size:.8125rem;display:inline-flex;align-items:center;min-height:40px}.custom-address[_ngcontent-%COMP%]{font-size:.75rem;opacity:.75}"]})};function Tw(t){return{start:t.x,center:t.x+t.width/2,end:t.x+t.width}}function Nw(t){return{start:t.y,center:t.y+t.height/2,end:t.y+t.height}}function kw(t,n){let e=null;for(let i of n)for(let r of[t.start,t.center,t.end])for(let o of[i.start,i.center,i.end]){let s=o-r;Math.abs(s)<=8&&(e===null||Math.abs(s)<Math.abs(e.offset))&&(e={offset:s,guideAt:o})}return e}function Aw(t,n,e){let i=[...n,e],r=i.map(Tw),o=i.map(Nw),s=kw(Tw(t),r),a=kw(Nw(t),o);return{x:t.x+(s?.offset??0),y:t.y+(a?.offset??0),verticalGuides:s?[s.guideAt]:[],horizontalGuides:a?[a.guideAt]:[]}}var Ps=16,Rw=24,Ow="zpl-label-studio.designer-draft.v1";function pL(){let t=us(Ow);return{sizePreset:dl(t?.sizePresetKey??WC),elements:t?.elements??[]}}var Lv={vertical:[],horizontal:[]},Nn=class t{layoutSignal=j(pL());selectedIdSignal=j(null);activeGuidesSignal=j(Lv);clipboardSignal=j(null);editingIdSignal=j(null);layout=this.layoutSignal.asReadonly();selectedId=this.selectedIdSignal.asReadonly();editingId=this.editingIdSignal.asReadonly();activeGuides=this.activeGuidesSignal.asReadonly();hasClipboard=Q(()=>this.clipboardSignal()!==null);constructor(){ut(()=>{let n=this.layoutSignal();fs(Ow,{sizePresetKey:n.sizePreset.key,elements:n.elements})})}loadLayout(n){this.layoutSignal.set(n),this.selectedIdSignal.set(null),this.activeGuidesSignal.set(Lv)}setSizePresetKey(n){this.layoutSignal.update(e=>k(v({},e),{sizePreset:dl(n)}))}addTextElement(){let n=this.layoutSignal(),{x:e,y:i}=this.cascadeCenter(n),r=KC({centerXDots:e,centerYDots:i});this.layoutSignal.set(ku(n,r)),this.select(r.id)}addImageElement(n,e,i){let r=this.layoutSignal(),o=Ki(r.sizePreset),s=Qi(r.sizePreset),{x:a,y:l}=this.cascadeCenter(r),c=JC({centerXDots:a,centerYDots:l,naturalWidth:e,naturalHeight:i,imageDataUrl:n,maxWidthDots:o*.8,maxHeightDots:s*.8});this.layoutSignal.set(ku(r,c)),this.select(c.id)}cascadeCenter(n){let i=n.elements.length%5*48;return{x:Ki(n.sizePreset)/2+i,y:Qi(n.sizePreset)/2+i}}removeElement(n){this.layoutSignal.update(e=>YC(e,n)),this.selectedIdSignal()===n&&this.selectedIdSignal.set(null)}updateElement(n,e){this.layoutSignal.update(i=>ZC(i,n,e))}setTextFontSize(n,e){let i=this.layoutSignal().elements.find(s=>s.id===n);if(i?.kind!=="text")return;let r=Math.max(Ps,e),o=Math.max(Ps,Math.round(e*1.2));this.updateElement(n,{fontSizeDots:e,widthDots:Math.max(i.widthDots,r),heightDots:Math.max(i.heightDots,o)})}select(n){this.selectedIdSignal.set(n),this.editingIdSignal()!==n&&this.editingIdSignal.set(null)}startEditing(n){this.selectedIdSignal.set(n),this.editingIdSignal.set(n)}stopEditing(){this.editingIdSignal.set(null)}nudgeElement(n,e,i){let r=this.layoutSignal().elements.find(o=>o.id===n);r&&this.updateElement(n,{xDots:Math.round(r.xDots+e),yDots:Math.round(r.yDots+i)})}moveElementWithSnap(n,e,i){let r=this.layoutSignal(),o=r.elements.find(u=>u.id===n);if(!o)return;let s={x:e,y:i,width:o.widthDots,height:o.heightDots},a=r.elements.filter(u=>u.id!==n).map(u=>({x:u.xDots,y:u.yDots,width:u.widthDots,height:u.heightDots})),l={x:0,y:0,width:Ki(r.sizePreset),height:Qi(r.sizePreset)},c=Aw(s,a,l);this.updateElement(n,{xDots:Math.round(c.x),yDots:Math.round(c.y)}),this.activeGuidesSignal.set({vertical:c.verticalGuides,horizontal:c.horizontalGuides})}resizeElement(n,e,i){let r=this.layoutSignal().elements.find(a=>a.id===n),o=r?.kind==="text"?Math.max(Ps,r.fontSizeDots):Ps,s=r?.kind==="text"?Math.max(Ps,Math.round(r.fontSizeDots*1.2)):Ps;this.updateElement(n,{widthDots:Math.max(o,Math.round(e)),heightDots:Math.max(s,Math.round(i))})}clearGuides(){this.activeGuidesSignal.set(Lv)}copySelected(){let n=this.layoutSignal().elements.find(e=>e.id===this.selectedIdSignal());n&&this.clipboardSignal.set(n)}pasteClipboard(){let n=this.clipboardSignal();if(!n)return;let e=QC(n,{xDots:n.xDots+Rw,yDots:n.yDots+Rw});this.layoutSignal.set(ku(this.layoutSignal(),e)),this.clipboardSignal.set(e),this.select(e.id)}duplicateSelected(){this.copySelected(),this.pasteClipboard()}static \u0275fac=function(e){return new(e||t)};static \u0275prov=G({token:t,factory:t.\u0275fac})};var gL=["textareaEl"];function vL(t,n){if(t&1){let e=Le();m(0,"textarea",4,0),D("blur",function(r){ee(e);let o=w(2);return te(o.onTextBlur(r))})("pointerdown",function(r){return r.stopPropagation()}),g()}if(t&2){let e=w(2);Jt("text-align",e.asText(e.element()).align)("font-weight",e.asText(e.element()).bold?"bold":"normal")("font-size",e.asText(e.element()).fontSizeDots*e.scale(),"px"),U("value",e.asText(e.element()).text)}}function bL(t,n){if(t&1&&(m(0,"div",6),y(1),g()),t&2){let e=n.$implicit;_(),Te(e)}}function _L(t,n){t&1&&(m(0,"div",7)(1,"mat-icon"),y(2,"warning"),g()())}function yL(t,n){if(t&1&&(m(0,"div",5),Xt(1,bL,2,1,"div",6,Lp),g(),V(3,_L,3,0,"div",7)),t&2){let e=w(2);Jt("width",e.textContentWidthPx(),"px")("height",e.textContentHeightPx(),"px")("transform",e.textContentTransform())("text-align",e.asText(e.element()).align)("font-weight",e.asText(e.element()).bold?"bold":"normal")("font-size",e.asText(e.element()).fontSizeDots*e.scale(),"px")("justify-content",e.verticalAlignToJustifyContent(e.asText(e.element()))),_(),Kt(e.visibleLines()),_(2),B(e.textOverflowing()?3:-1)}}function SL(t,n){if(t&1&&V(0,vL,2,7,"textarea",3)(1,yL,4,15),t&2){let e=w();B(e.editing()?0:1)}}function CL(t,n){if(t&1&&ie(0,"img",1),t&2){let e=w();U("src",e.asImage(e.element()).imageDataUrl,up)}}var Vv=null;function DL(){return Vv||(Vv=document.createElement("canvas").getContext("2d")),Vv}var Rf=class t{store=d(Nn);element=yn.required();scale=yn.required();editing=Q(()=>this.store.editingId()===this.element().id);isSelected=Q(()=>this.store.selectedId()===this.element().id);textareaRef=ui("textareaEl");effectiveSize=Q(()=>{let n=this.element();return n.kind!=="text"?{width:n.widthDots,height:n.heightDots}:Pu(n.widthDots,n.heightDots,n.rotationDeg)});allLines=Q(()=>{let n=this.element();if(n.kind!=="text")return[];let e=DL();return e.font=`${n.bold?"bold ":""}${n.fontSizeDots}px Arial, sans-serif`,Ru(n.text,this.effectiveSize().width,i=>e.measureText(i).width)});visibleLines=Q(()=>{let n=this.element();return n.kind!=="text"?[]:this.allLines().slice(0,Ou(this.effectiveSize().height,n.fontSizeDots))});textContentWidthPx=Q(()=>this.effectiveSize().width*this.scale());textContentHeightPx=Q(()=>this.effectiveSize().height*this.scale());textContentTransform=Q(()=>{let n=this.element();return`translate(-50%, -50%) rotate(${n.kind==="text"?n.rotationDeg:0}deg)`});textOverflowing=Q(()=>this.allLines().length>this.visibleLines().length);constructor(){ut(()=>{let n=this.textareaRef()?.nativeElement;this.editing()&&n&&(n.focus(),n.select())})}asText(n){return n}verticalAlignToJustifyContent(n){switch(n.verticalAlign){case"middle":return"center";case"bottom":return"flex-end";default:return"flex-start"}}asImage(n){return n}onPointerDownMove(n){if(n.target.closest(".resize-handle")||(n.stopPropagation(),this.store.select(this.element().id),this.editing()))return;let e=n.clientX,i=n.clientY,r=this.element().xDots,o=this.element().yDots,s=this.scale(),a=this.element().id,l=u=>{let f=(u.clientX-e)/s,h=(u.clientY-i)/s;this.store.moveElementWithSnap(a,r+f,o+h)},c=()=>{this.store.clearGuides(),window.removeEventListener("pointermove",l),window.removeEventListener("pointerup",c)};window.addEventListener("pointermove",l),window.addEventListener("pointerup",c)}onResizeHandlePointerDown(n){n.stopPropagation(),n.preventDefault();let e=n.clientX,i=n.clientY,r=this.element().widthDots,o=this.element().heightDots,s=this.scale(),a=this.element().id,l=u=>{let f=(u.clientX-e)/s,h=(u.clientY-i)/s;this.store.resizeElement(a,r+f,o+h)},c=()=>{window.removeEventListener("pointermove",l),window.removeEventListener("pointerup",c)};window.addEventListener("pointermove",l),window.addEventListener("pointerup",c)}onDoubleClick(){this.element().kind==="text"&&this.store.startEditing(this.element().id)}onTextBlur(n){let e=n.target.value;this.store.updateElement(this.element().id,{text:e}),this.store.stopEditing()}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=T({type:t,selectors:[["app-label-element-box"]],viewQuery:function(e,i){e&1&&$i(i.textareaRef,gL,5),e&2&&Gi()},hostAttrs:[1,"label-element-box"],hostVars:10,hostBindings:function(e,i){e&1&&D("pointerdown",function(o){return i.onPointerDownMove(o)})("dblclick",function(){return i.onDoubleClick()}),e&2&&(Jt("left",i.element().xDots*i.scale(),"px")("top",i.element().yDots*i.scale(),"px")("width",i.element().widthDots*i.scale(),"px")("height",i.element().heightDots*i.scale(),"px"),z("is-selected",i.isSelected()))},inputs:{element:[1,"element"],scale:[1,"scale"]},decls:3,vars:1,consts:[["textareaEl",""],["alt","",1,"image-content",3,"src"],[1,"resize-handle",3,"pointerdown"],[1,"text-edit-overlay",3,"value","text-align","font-weight","font-size"],[1,"text-edit-overlay",3,"blur","pointerdown","value"],[1,"text-content"],[1,"text-line"],["matTooltip","Parte do texto n\xE3o cabe nessa caixa e n\xE3o vai aparecer \u2014 nem aqui na tela, nem na etiqueta impressa. Aumente a caixa ou diminua o tamanho da fonte pra mostrar tudo.","matTooltipPosition","above",1,"overflow-warning"]],template:function(e,i){e&1&&(V(0,SL,2,1)(1,CL,1,1,"img",1),m(2,"div",2),D("pointerdown",function(o){return i.onResizeHandlePointerDown(o)}),g()),e&2&&B(i.element().kind==="text"?0:1)},dependencies:[Zn,Yn,kf,zl],styles:['[_nghost-%COMP%]{position:absolute;box-sizing:border-box;border:1px dashed rgba(0,0,0,.2);cursor:move;-webkit-user-select:none;user-select:none;touch-action:none}.is-selected[_nghost-%COMP%]{border-color:#3f51b5}.text-content[_ngcontent-%COMP%]{position:absolute;top:50%;left:50%;overflow:hidden;display:flex;flex-direction:column;color:#000;font-family:Arial,sans-serif;line-height:1.2}.text-line[_ngcontent-%COMP%]{white-space:pre}.text-edit-overlay[_ngcontent-%COMP%]{width:100%;height:100%;box-sizing:border-box;border:none;outline:none;resize:none;font-family:Arial,sans-serif;padding:0}.image-content[_ngcontent-%COMP%]{width:100%;height:100%;object-fit:fill;display:block;pointer-events:none}.overflow-warning[_ngcontent-%COMP%]{position:absolute;top:2px;right:2px;color:#f57c00;background:#fff;border-radius:50%;line-height:0;cursor:help}.overflow-warning[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:1rem;width:1rem;height:1rem}.resize-handle[_ngcontent-%COMP%]{position:absolute;right:-5px;bottom:-5px;width:10px;height:10px;border-radius:50%;background:#3f51b5;cursor:nwse-resize;display:none}.resize-handle[_ngcontent-%COMP%]:before{content:"";position:absolute;inset:-10px -22px -22px -10px}.is-selected[_nghost-%COMP%]   .resize-handle[_ngcontent-%COMP%]{display:block}']})};var EL=["button"],wL=["*"];function xL(t,n){if(t&1&&(m(0,"div",2),ie(1,"mat-pseudo-checkbox",6),g()),t&2){let e=w();_(),U("disabled",e.disabled)}}var Pw=new b("MAT_BUTTON_TOGGLE_DEFAULT_OPTIONS",{providedIn:"root",factory:()=>({hideSingleSelectionIndicator:!1,hideMultipleSelectionIndicator:!1,disabledInteractive:!1})}),Fw=new b("MatButtonToggleGroup"),IL={provide:Ll,useExisting:Pt(()=>Bv),multi:!0},Of=class{source;value;constructor(n,e){this.source=n,this.value=e}},Bv=(()=>{class t{_changeDetector=d(Oe);_dir=d(ct,{optional:!0});_multiple=!1;_disabled=!1;_disabledInteractive=!1;_selectionModel;_rawValue;_controlValueAccessorChangeFn=()=>{};_onTouched=()=>{};_buttonToggles;appearance;get name(){return this._name}set name(e){this._name=e,this._markButtonsForCheck()}_name=d(xe).getId("mat-button-toggle-group-");vertical=!1;get value(){let e=this._selectionModel?this._selectionModel.selected:[];return this.multiple?e.map(i=>i.value):e[0]?e[0].value:void 0}set value(e){this._setSelectionByValue(e),this.valueChange.emit(this.value)}valueChange=new W;get selected(){let e=this._selectionModel?this._selectionModel.selected:[];return this.multiple?e:e[0]||null}get multiple(){return this._multiple}set multiple(e){this._multiple=e,this._markButtonsForCheck()}get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._markButtonsForCheck()}get disabledInteractive(){return this._disabledInteractive}set disabledInteractive(e){this._disabledInteractive=e,this._markButtonsForCheck()}get dir(){return this._dir&&this._dir.value==="rtl"?"rtl":"ltr"}change=new W;get hideSingleSelectionIndicator(){return this._hideSingleSelectionIndicator}set hideSingleSelectionIndicator(e){this._hideSingleSelectionIndicator=e,this._markButtonsForCheck()}_hideSingleSelectionIndicator;get hideMultipleSelectionIndicator(){return this._hideMultipleSelectionIndicator}set hideMultipleSelectionIndicator(e){this._hideMultipleSelectionIndicator=e,this._markButtonsForCheck()}_hideMultipleSelectionIndicator;constructor(){let e=d(Pw,{optional:!0});this.appearance=e&&e.appearance?e.appearance:"standard",this._hideSingleSelectionIndicator=e?.hideSingleSelectionIndicator??!1,this._hideMultipleSelectionIndicator=e?.hideMultipleSelectionIndicator??!1}ngOnInit(){this._selectionModel=new _o(this.multiple,void 0,!1)}ngAfterContentInit(){this._selectionModel.select(...this._buttonToggles.filter(e=>e.checked)),this.multiple||this._initializeTabIndex()}writeValue(e){this.value=e,this._changeDetector.markForCheck()}registerOnChange(e){this._controlValueAccessorChangeFn=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e}_keydown(e){if(this.multiple||this.disabled||$e(e))return;let r=e.target.id,o=this._buttonToggles.toArray().findIndex(a=>a.buttonId===r),s=null;switch(e.keyCode){case 32:case 13:s=this._buttonToggles.get(o)||null;break;case 38:s=this._getNextButton(o,-1);break;case 37:s=this._getNextButton(o,this.dir==="ltr"?-1:1);break;case 40:s=this._getNextButton(o,1);break;case 39:s=this._getNextButton(o,this.dir==="ltr"?1:-1);break;default:return}s&&(e.preventDefault(),s._onButtonClick(),s.focus())}_emitChangeEvent(e){let i=new Of(e,this.value);this._rawValue=i.value,this._controlValueAccessorChangeFn(i.value),this.change.emit(i)}_syncButtonToggle(e,i,r=!1,o=!1){!this.multiple&&this.selected&&!e.checked&&(this.selected.checked=!1),this._selectionModel?i?this._selectionModel.select(e):this._selectionModel.deselect(e):o=!0,o?Promise.resolve().then(()=>this._updateModelValue(e,r)):this._updateModelValue(e,r)}_isSelected(e){return this._selectionModel&&this._selectionModel.isSelected(e)}_isPrechecked(e){return typeof this._rawValue>"u"?!1:this.multiple&&Array.isArray(this._rawValue)?this._rawValue.some(i=>e.value!=null&&i===e.value):e.value===this._rawValue}_initializeTabIndex(){if(this._buttonToggles.forEach(e=>{e.tabIndex=-1}),this.selected)this.selected.tabIndex=0;else for(let e=0;e<this._buttonToggles.length;e++){let i=this._buttonToggles.get(e);if(!i.disabled){i.tabIndex=0;break}}}_getNextButton(e,i){let r=this._buttonToggles;for(let o=1;o<=r.length;o++){let s=(e+i*o+r.length)%r.length,a=r.get(s);if(a&&!a.disabled)return a}return null}_setSelectionByValue(e){if(this._rawValue=e,!this._buttonToggles)return;let i=this._buttonToggles.toArray();if(this.multiple&&e?(Array.isArray(e),this._clearSelection(),e.forEach(r=>this._selectValue(r,i))):(this._clearSelection(),this._selectValue(e,i)),!this.multiple&&i.every(r=>r.tabIndex===-1)){for(let r of i)if(!r.disabled){r.tabIndex=0;break}}}_clearSelection(){this._selectionModel.clear(),this._buttonToggles.forEach(e=>{e.checked=!1,this.multiple||(e.tabIndex=-1)})}_selectValue(e,i){for(let r of i)if(r.value===e){r.checked=!0,this._selectionModel.select(r),this.multiple||(r.tabIndex=0);break}}_updateModelValue(e,i){i&&this._emitChangeEvent(e),this.valueChange.emit(this.value)}_markButtonsForCheck(){this._buttonToggles?.forEach(e=>e._markForCheck())}static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,selectors:[["mat-button-toggle-group"]],contentQueries:function(i,r,o){if(i&1&&zn(o,Pf,5),i&2){let s;ce(s=de())&&(r._buttonToggles=s)}},hostAttrs:[1,"mat-button-toggle-group"],hostVars:6,hostBindings:function(i,r){i&1&&D("keydown",function(s){return r._keydown(s)}),i&2&&(re("role",r.multiple?"group":"radiogroup")("aria-disabled",r.disabled),z("mat-button-toggle-vertical",r.vertical)("mat-button-toggle-group-appearance-standard",r.appearance==="standard"))},inputs:{appearance:"appearance",name:"name",vertical:[2,"vertical","vertical",P],value:"value",multiple:[2,"multiple","multiple",P],disabled:[2,"disabled","disabled",P],disabledInteractive:[2,"disabledInteractive","disabledInteractive",P],hideSingleSelectionIndicator:[2,"hideSingleSelectionIndicator","hideSingleSelectionIndicator",P],hideMultipleSelectionIndicator:[2,"hideMultipleSelectionIndicator","hideMultipleSelectionIndicator",P]},outputs:{valueChange:"valueChange",change:"change"},exportAs:["matButtonToggleGroup"],features:[Be([IL,{provide:Fw,useExisting:t}])]})}return t})(),Pf=(()=>{class t{_changeDetectorRef=d(Oe);_elementRef=d(O);_focusMonitor=d(rn);_idGenerator=d(xe);_animationDisabled=Ce();_checked=!1;ariaLabel;ariaLabelledby=null;_buttonElement;buttonToggleGroup;get buttonId(){return`${this.id}-button`}id;name;value;get tabIndex(){return this._tabIndex()}set tabIndex(e){this._tabIndex.set(e)}_tabIndex;disableRipple=!1;get appearance(){return this.buttonToggleGroup?this.buttonToggleGroup.appearance:this._appearance}set appearance(e){this._appearance=e}_appearance;get checked(){return this.buttonToggleGroup?this.buttonToggleGroup._isSelected(this):this._checked}set checked(e){e!==this._checked&&(this._checked=e,this.buttonToggleGroup&&this.buttonToggleGroup._syncButtonToggle(this,this._checked),this._changeDetectorRef.markForCheck())}get disabled(){return this._disabled||this.buttonToggleGroup&&this.buttonToggleGroup.disabled}set disabled(e){this._disabled=e}_disabled=!1;get disabledInteractive(){return this._disabledInteractive||this.buttonToggleGroup!==null&&this.buttonToggleGroup.disabledInteractive}set disabledInteractive(e){this._disabledInteractive=e}_disabledInteractive;change=new W;constructor(){d(ze).load(qn);let e=d(Fw,{optional:!0}),i=d(new _n("tabindex"),{optional:!0})||"",r=d(Pw,{optional:!0});this._tabIndex=j(parseInt(i)||0),this.buttonToggleGroup=e,this._appearance=r&&r.appearance?r.appearance:"standard",this._disabledInteractive=r?.disabledInteractive??!1}ngOnInit(){let e=this.buttonToggleGroup;this.id=this.id||this._idGenerator.getId("mat-button-toggle-"),e&&(e._isPrechecked(this)?this.checked=!0:e._isSelected(this)!==this._checked&&e._syncButtonToggle(this,this._checked))}ngAfterViewInit(){this._animationDisabled||this._elementRef.nativeElement.classList.add("mat-button-toggle-animations-enabled"),this._focusMonitor.monitor(this._elementRef,!0)}ngOnDestroy(){let e=this.buttonToggleGroup;this._focusMonitor.stopMonitoring(this._elementRef),e&&e._isSelected(this)&&e._syncButtonToggle(this,!1,!1,!0)}focus(e){this._buttonElement.nativeElement.focus(e)}_onButtonClick(){if(this.disabled)return;let e=this.isSingleSelector()?!0:!this._checked;if(e!==this._checked&&(this._checked=e,this.buttonToggleGroup&&(this.buttonToggleGroup._syncButtonToggle(this,this._checked,!0),this.buttonToggleGroup._onTouched())),this.isSingleSelector()){let i=this.buttonToggleGroup._buttonToggles.find(r=>r.tabIndex===0);i&&(i.tabIndex=-1),this.tabIndex=0}this.change.emit(new Of(this,this.value))}_markForCheck(){this._changeDetectorRef.markForCheck()}_getButtonName(){return this.isSingleSelector()?this.buttonToggleGroup.name:this.name||null}isSingleSelector(){return this.buttonToggleGroup&&!this.buttonToggleGroup.multiple}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-button-toggle"]],viewQuery:function(i,r){if(i&1&&Ue(EL,5),i&2){let o;ce(o=de())&&(r._buttonElement=o.first)}},hostAttrs:["role","presentation",1,"mat-button-toggle"],hostVars:14,hostBindings:function(i,r){i&1&&D("focus",function(){return r.focus()}),i&2&&(re("aria-label",null)("aria-labelledby",null)("id",r.id)("name",null),z("mat-button-toggle-standalone",!r.buttonToggleGroup)("mat-button-toggle-checked",r.checked)("mat-button-toggle-disabled",r.disabled)("mat-button-toggle-disabled-interactive",r.disabledInteractive)("mat-button-toggle-appearance-standard",r.appearance==="standard"))},inputs:{ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],id:"id",name:"name",value:"value",tabIndex:"tabIndex",disableRipple:[2,"disableRipple","disableRipple",P],appearance:"appearance",checked:[2,"checked","checked",P],disabled:[2,"disabled","disabled",P],disabledInteractive:[2,"disabledInteractive","disabledInteractive",P]},outputs:{change:"change"},exportAs:["matButtonToggle"],ngContentSelectors:wL,decls:7,vars:13,consts:[["button",""],["type","button",1,"mat-button-toggle-button","mat-focus-indicator",3,"click","id","disabled"],[1,"mat-button-toggle-checkbox-wrapper"],[1,"mat-button-toggle-label-content"],[1,"mat-button-toggle-focus-overlay"],["matRipple","",1,"mat-button-toggle-ripple",3,"matRippleTrigger","matRippleDisabled"],["state","checked","aria-hidden","true","appearance","minimal",3,"disabled"]],template:function(i,r){if(i&1&&(Pe(),m(0,"button",1,0),D("click",function(){return r._onButtonClick()}),V(2,xL,2,1,"div",2),m(3,"span",3),he(4),g()(),ie(5,"span",4)(6,"span",5)),i&2){let o=ht(1);U("id",r.buttonId)("disabled",r.disabled&&!r.disabledInteractive||null),re("role",r.isSingleSelector()?"radio":"button")("tabindex",r.disabled&&!r.disabledInteractive?-1:r.tabIndex)("aria-pressed",r.isSingleSelector()?null:r.checked)("aria-checked",r.isSingleSelector()?r.checked:null)("name",r._getButtonName())("aria-label",r.ariaLabel)("aria-labelledby",r.ariaLabelledby)("aria-disabled",r.disabled&&r.disabledInteractive?"true":null),_(2),B(r.buttonToggleGroup&&(!r.buttonToggleGroup.multiple&&!r.buttonToggleGroup.hideSingleSelectionIndicator||r.buttonToggleGroup.multiple&&!r.buttonToggleGroup.hideMultipleSelectionIndicator)?2:-1),_(4),U("matRippleTrigger",o)("matRippleDisabled",r.disableRipple||r.disabled)}},dependencies:[sr,Df],styles:[`.mat-button-toggle-standalone,
.mat-button-toggle-group {
  position: relative;
  display: inline-flex;
  flex-direction: row;
  white-space: nowrap;
  overflow: hidden;
  -webkit-tap-highlight-color: transparent;
  border-radius: var(--%NS%mat-button-toggle-legacy-shape);
  transform: translateZ(0);
}
.mat-button-toggle-standalone:not([class*=mat-elevation-z]),
.mat-button-toggle-group:not([class*=mat-elevation-z]) {
  box-shadow: 0px 3px 1px -2px rgba(0, 0, 0, 0.2), 0px 2px 2px 0px rgba(0, 0, 0, 0.14), 0px 1px 5px 0px rgba(0, 0, 0, 0.12);
}
@media (forced-colors: active) {
  .mat-button-toggle-standalone,
  .mat-button-toggle-group {
    outline: solid 1px;
  }
}

.mat-button-toggle-standalone.mat-button-toggle-appearance-standard,
.mat-button-toggle-group-appearance-standard {
  border-radius: var(--%NS%mat-button-toggle-shape, var(--%NS%mat-sys-corner-extra-large));
  border: solid 1px var(--%NS%mat-button-toggle-divider-color, var(--%NS%mat-sys-outline));
}
.mat-button-toggle-standalone.mat-button-toggle-appearance-standard .mat-pseudo-checkbox,
.mat-button-toggle-group-appearance-standard .mat-pseudo-checkbox {
  --%NS%mat-pseudo-checkbox-minimal-selected-checkmark-color: var(--%NS%mat-button-toggle-selected-state-text-color, var(--%NS%mat-sys-on-secondary-container));
}
.mat-button-toggle-standalone.mat-button-toggle-appearance-standard:not([class*=mat-elevation-z]),
.mat-button-toggle-group-appearance-standard:not([class*=mat-elevation-z]) {
  box-shadow: none;
}
@media (forced-colors: active) {
  .mat-button-toggle-standalone.mat-button-toggle-appearance-standard,
  .mat-button-toggle-group-appearance-standard {
    outline: 0;
  }
}

.mat-button-toggle-vertical {
  flex-direction: column;
}
.mat-button-toggle-vertical .mat-button-toggle-label-content {
  display: block;
}

.mat-button-toggle {
  white-space: nowrap;
  position: relative;
  color: var(--%NS%mat-button-toggle-legacy-text-color);
  font-family: var(--%NS%mat-button-toggle-legacy-label-text-font);
  font-size: var(--%NS%mat-button-toggle-legacy-label-text-size);
  line-height: var(--%NS%mat-button-toggle-legacy-label-text-line-height);
  font-weight: var(--%NS%mat-button-toggle-legacy-label-text-weight);
  letter-spacing: var(--%NS%mat-button-toggle-legacy-label-text-tracking);
  --%NS%mat-pseudo-checkbox-minimal-selected-checkmark-color: var(--%NS%mat-button-toggle-legacy-selected-state-text-color);
}
.mat-button-toggle.cdk-keyboard-focused .mat-button-toggle-focus-overlay {
  opacity: var(--%NS%mat-button-toggle-legacy-focus-state-layer-opacity);
}
.mat-button-toggle .mat-icon svg {
  vertical-align: top;
}

.mat-button-toggle-checkbox-wrapper {
  display: inline-block;
  justify-content: flex-start;
  align-items: center;
  width: 0;
  height: 18px;
  line-height: 18px;
  overflow: hidden;
  box-sizing: border-box;
  position: absolute;
  top: 50%;
  left: 16px;
  transform: translate3d(0, -50%, 0);
}
[dir=rtl] .mat-button-toggle-checkbox-wrapper {
  left: auto;
  right: 16px;
}
.mat-button-toggle-appearance-standard .mat-button-toggle-checkbox-wrapper {
  left: 12px;
}
[dir=rtl] .mat-button-toggle-appearance-standard .mat-button-toggle-checkbox-wrapper {
  left: auto;
  right: 12px;
}
.mat-button-toggle-checked .mat-button-toggle-checkbox-wrapper {
  width: 18px;
}
.mat-button-toggle-animations-enabled .mat-button-toggle-checkbox-wrapper {
  transition: width 150ms 45ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-button-toggle-vertical .mat-button-toggle-checkbox-wrapper {
  transition: none;
}

.mat-button-toggle-checked {
  color: var(--%NS%mat-button-toggle-legacy-selected-state-text-color);
  background-color: var(--%NS%mat-button-toggle-legacy-selected-state-background-color);
}

.mat-button-toggle-disabled {
  pointer-events: none;
  color: var(--%NS%mat-button-toggle-legacy-disabled-state-text-color);
  background-color: var(--%NS%mat-button-toggle-legacy-disabled-state-background-color);
  --%NS%mat-pseudo-checkbox-minimal-disabled-selected-checkmark-color: var(--%NS%mat-button-toggle-legacy-disabled-state-text-color);
}
.mat-button-toggle-disabled.mat-button-toggle-checked {
  background-color: var(--%NS%mat-button-toggle-legacy-disabled-selected-state-background-color);
}

.mat-button-toggle-disabled-interactive {
  pointer-events: auto;
}

.mat-button-toggle-appearance-standard {
  color: var(--%NS%mat-button-toggle-text-color, var(--%NS%mat-sys-on-surface));
  background-color: var(--%NS%mat-button-toggle-background-color, transparent);
  font-family: var(--%NS%mat-button-toggle-label-text-font, var(--%NS%mat-sys-label-large-font));
  font-size: var(--%NS%mat-button-toggle-label-text-size, var(--%NS%mat-sys-label-large-size));
  line-height: var(--%NS%mat-button-toggle-label-text-line-height, var(--%NS%mat-sys-label-large-line-height));
  font-weight: var(--%NS%mat-button-toggle-label-text-weight, var(--%NS%mat-sys-label-large-weight));
  letter-spacing: var(--%NS%mat-button-toggle-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
}
.mat-button-toggle-group-appearance-standard .mat-button-toggle-appearance-standard + .mat-button-toggle-appearance-standard {
  border-left: solid 1px var(--%NS%mat-button-toggle-divider-color, var(--%NS%mat-sys-outline));
}
[dir=rtl] .mat-button-toggle-group-appearance-standard .mat-button-toggle-appearance-standard + .mat-button-toggle-appearance-standard {
  border-left: none;
  border-right: solid 1px var(--%NS%mat-button-toggle-divider-color, var(--%NS%mat-sys-outline));
}
.mat-button-toggle-group-appearance-standard.mat-button-toggle-vertical .mat-button-toggle-appearance-standard + .mat-button-toggle-appearance-standard {
  border-left: none;
  border-right: none;
  border-top: solid 1px var(--%NS%mat-button-toggle-divider-color, var(--%NS%mat-sys-outline));
}
.mat-button-toggle-appearance-standard.mat-button-toggle-checked {
  color: var(--%NS%mat-button-toggle-selected-state-text-color, var(--%NS%mat-sys-on-secondary-container));
  background-color: var(--%NS%mat-button-toggle-selected-state-background-color, var(--%NS%mat-sys-secondary-container));
}
.mat-button-toggle-appearance-standard.mat-button-toggle-disabled {
  color: var(--%NS%mat-button-toggle-disabled-state-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  background-color: var(--%NS%mat-button-toggle-disabled-state-background-color, transparent);
}
.mat-button-toggle-appearance-standard.mat-button-toggle-disabled .mat-pseudo-checkbox {
  --%NS%mat-pseudo-checkbox-minimal-disabled-selected-checkmark-color: var(--%NS%mat-button-toggle-disabled-selected-state-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-button-toggle-appearance-standard.mat-button-toggle-disabled.mat-button-toggle-checked {
  color: var(--%NS%mat-button-toggle-disabled-selected-state-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  background-color: var(--%NS%mat-button-toggle-disabled-selected-state-background-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mat-button-toggle-appearance-standard .mat-button-toggle-focus-overlay {
  background-color: var(--%NS%mat-button-toggle-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mat-button-toggle-appearance-standard:hover .mat-button-toggle-focus-overlay {
  opacity: var(--%NS%mat-button-toggle-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-button-toggle-appearance-standard.cdk-keyboard-focused .mat-button-toggle-focus-overlay {
  opacity: var(--%NS%mat-button-toggle-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
@media (hover: none) {
  .mat-button-toggle-appearance-standard:hover .mat-button-toggle-focus-overlay {
    display: none;
  }
}

.mat-button-toggle-label-content {
  -webkit-user-select: none;
  user-select: none;
  display: inline-block;
  padding: 0 16px;
  line-height: var(--%NS%mat-button-toggle-legacy-height);
  position: relative;
}
.mat-button-toggle-appearance-standard .mat-button-toggle-label-content {
  padding: 0 12px;
  line-height: var(--%NS%mat-button-toggle-height, 40px);
}

.mat-button-toggle-label-content > * {
  vertical-align: middle;
}

.mat-button-toggle-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
  pointer-events: none;
  opacity: 0;
  background-color: var(--%NS%mat-button-toggle-legacy-state-layer-color);
}

@media (forced-colors: active) {
  .mat-button-toggle-checked .mat-button-toggle-focus-overlay {
    border-bottom: solid 500px;
    opacity: 0.5;
    height: 0;
  }
  .mat-button-toggle-checked:hover .mat-button-toggle-focus-overlay {
    opacity: 0.6;
  }
  .mat-button-toggle-checked.mat-button-toggle-appearance-standard .mat-button-toggle-focus-overlay {
    border-bottom: solid 500px;
  }
}
.mat-button-toggle .mat-button-toggle-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}

.mat-button-toggle-button {
  border: 0;
  background: none;
  color: inherit;
  padding: 0;
  margin: 0;
  font: inherit;
  outline: none;
  width: 100%;
  cursor: pointer;
}
.mat-button-toggle-animations-enabled .mat-button-toggle-button {
  transition: padding 150ms 45ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-button-toggle-vertical .mat-button-toggle-button {
  transition: none;
}
.mat-button-toggle-disabled .mat-button-toggle-button {
  cursor: default;
}
.mat-button-toggle-button::-moz-focus-inner {
  border: 0;
}
.mat-button-toggle-checked .mat-button-toggle-button:has(.mat-button-toggle-checkbox-wrapper) {
  padding-left: 30px;
}
[dir=rtl] .mat-button-toggle-checked .mat-button-toggle-button:has(.mat-button-toggle-checkbox-wrapper) {
  padding-left: 0;
  padding-right: 30px;
}

.mat-button-toggle-standalone.mat-button-toggle-appearance-standard {
  --%NS%mat-focus-indicator-border-radius: var(--%NS%mat-button-toggle-shape, var(--%NS%mat-sys-corner-extra-large));
}

.mat-button-toggle-group-appearance-standard:not(.mat-button-toggle-vertical) .mat-button-toggle:last-of-type .mat-button-toggle-button::before {
  border-top-right-radius: var(--%NS%mat-button-toggle-shape, var(--%NS%mat-sys-corner-extra-large));
  border-bottom-right-radius: var(--%NS%mat-button-toggle-shape, var(--%NS%mat-sys-corner-extra-large));
}
.mat-button-toggle-group-appearance-standard:not(.mat-button-toggle-vertical) .mat-button-toggle:first-of-type .mat-button-toggle-button::before {
  border-top-left-radius: var(--%NS%mat-button-toggle-shape, var(--%NS%mat-sys-corner-extra-large));
  border-bottom-left-radius: var(--%NS%mat-button-toggle-shape, var(--%NS%mat-sys-corner-extra-large));
}

.mat-button-toggle-group-appearance-standard.mat-button-toggle-vertical .mat-button-toggle:last-of-type .mat-button-toggle-button::before {
  border-bottom-right-radius: var(--%NS%mat-button-toggle-shape, var(--%NS%mat-sys-corner-extra-large));
  border-bottom-left-radius: var(--%NS%mat-button-toggle-shape, var(--%NS%mat-sys-corner-extra-large));
}
.mat-button-toggle-group-appearance-standard.mat-button-toggle-vertical .mat-button-toggle:first-of-type .mat-button-toggle-button::before {
  border-top-right-radius: var(--%NS%mat-button-toggle-shape, var(--%NS%mat-sys-corner-extra-large));
  border-top-left-radius: var(--%NS%mat-button-toggle-shape, var(--%NS%mat-sys-corner-extra-large));
}
`],encapsulation:2})}return t})(),Lw=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({imports:[ar,Pf,ve]})}return t})();var TL=["*"],Vw=(()=>{class t{labelPosition="after";static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["","mat-internal-form-field",""]],hostAttrs:[1,"mdc-form-field","mat-internal-form-field"],hostVars:2,hostBindings:function(i,r){i&2&&z("mdc-form-field--align-end",r.labelPosition==="before")},inputs:{labelPosition:"labelPosition"},ngContentSelectors:TL,decls:1,vars:0,template:function(i,r){i&1&&(Pe(),he(0))},styles:[`.mat-internal-form-field {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  display: inline-flex;
  align-items: center;
  vertical-align: middle;
}
.mat-internal-form-field > label, .mat-internal-form-field > .mat-internal-form-field-label {
  margin-left: 0;
  margin-right: auto;
  padding-left: 4px;
  padding-right: 0;
  order: 0;
}
[dir=rtl] .mat-internal-form-field > label, [dir=rtl] .mat-internal-form-field > .mat-internal-form-field-label {
  margin-left: auto;
  margin-right: 0;
  padding-left: 0;
  padding-right: 4px;
}

.mdc-form-field--align-end > label, .mdc-form-field--align-end > .mat-internal-form-field-label {
  margin-left: auto;
  margin-right: 0;
  padding-left: 0;
  padding-right: 4px;
  order: -1;
}
[dir=rtl] .mdc-form-field--align-end .mdc-form-field--align-end label, [dir=rtl] .mdc-form-field--align-end .mdc-form-field--align-end .mat-internal-form-field-label {
  margin-left: 0;
  margin-right: auto;
  padding-left: 4px;
  padding-right: 0;
}
`],encapsulation:2})}return t})();var NL=["input"],kL=["*"],jv={color:"accent",clickAction:"check-indeterminate",disabledInteractive:!1},AL=new b("mat-checkbox-default-options",{providedIn:"root",factory:()=>jv}),It=(function(t){return t[t.Init=0]="Init",t[t.Checked=1]="Checked",t[t.Unchecked=2]="Unchecked",t[t.Indeterminate=3]="Indeterminate",t})(It||{}),Hv=class{source;checked},Uv=(()=>{class t{_elementRef=d(O);_changeDetectorRef=d(Oe);_ngZone=d(N);_animationsDisabled=Ce();_options=d(AL,{optional:!0});focus(){this._inputElement.nativeElement.focus()}_createChangeEvent(e){let i=new Hv;return i.source=this,i.checked=e,i}_getAnimationTargetElement(){return this._inputElement?.nativeElement}_animationClasses={uncheckedToChecked:"mdc-checkbox--anim-unchecked-checked",uncheckedToIndeterminate:"mdc-checkbox--anim-unchecked-indeterminate",checkedToUnchecked:"mdc-checkbox--anim-checked-unchecked",checkedToIndeterminate:"mdc-checkbox--anim-checked-indeterminate",indeterminateToChecked:"mdc-checkbox--anim-indeterminate-checked",indeterminateToUnchecked:"mdc-checkbox--anim-indeterminate-unchecked"};ariaLabel="";ariaLabelledby=null;ariaDescribedby;ariaExpanded;ariaControls;ariaOwns;_uniqueId;id;get inputId(){return`${this.id||this._uniqueId}-input`}required=!1;labelPosition="after";name=null;change=new W;indeterminateChange=new W;value;disableRipple=!1;_inputElement;tabIndex;color;disabledInteractive;_onTouched=()=>{};_currentAnimationClass="";_currentCheckState=It.Init;_controlValueAccessorChangeFn=()=>{};_validatorChangeFn=()=>{};constructor(){d(ze).load(qn);let e=d(new _n("tabindex"),{optional:!0});this._options=this._options||jv,this.color=this._options.color||jv.color,this.tabIndex=e==null?0:parseInt(e)||0,this.id=this._uniqueId=d(xe).getId("mat-mdc-checkbox-"),this.disabledInteractive=this._options?.disabledInteractive??!1}ngOnChanges(e){e.required&&this._validatorChangeFn()}ngAfterViewInit(){this._syncIndeterminate(this.indeterminate)}get checked(){return this._checked}set checked(e){e!=this.checked&&(this._checked=e,this._changeDetectorRef.markForCheck())}_checked=!1;get disabled(){return this._disabled}set disabled(e){e!==this.disabled&&(this._disabled=e,this._changeDetectorRef.markForCheck())}_disabled=!1;get indeterminate(){return this._indeterminate()}set indeterminate(e){let i=e!=this._indeterminate();this._indeterminate.set(e),i&&(e?this._transitionCheckState(It.Indeterminate):this._transitionCheckState(this.checked?It.Checked:It.Unchecked),this.indeterminateChange.emit(e)),this._syncIndeterminate(e)}_indeterminate=j(!1);_isRippleDisabled(){return this.disableRipple||this.disabled}_onLabelTextChange(){this._changeDetectorRef.detectChanges()}writeValue(e){this.checked=!!e}registerOnChange(e){this._controlValueAccessorChangeFn=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e}validate(e){return this.required&&e.value!==!0?{required:!0}:null}registerOnValidatorChange(e){this._validatorChangeFn=e}_transitionCheckState(e){let i=this._currentCheckState,r=this._getAnimationTargetElement();if(!(i===e||!r)&&(this._currentAnimationClass&&r.classList.remove(this._currentAnimationClass),this._currentAnimationClass=this._getAnimationClassForCheckStateTransition(i,e),this._currentCheckState=e,this._currentAnimationClass.length>0)){r.classList.add(this._currentAnimationClass);let o=this._currentAnimationClass;this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{r.classList.remove(o)},1e3)})}}_emitChangeEvent(){this._controlValueAccessorChangeFn(this.checked),this.change.emit(this._createChangeEvent(this.checked)),this._inputElement&&(this._inputElement.nativeElement.checked=this.checked)}toggle(){this.checked=!this.checked,this._controlValueAccessorChangeFn(this.checked)}_handleInputClick(){let e=this._options?.clickAction;!this.disabled&&e!=="noop"?(this.indeterminate&&e!=="check"&&Promise.resolve().then(()=>{this._indeterminate.set(!1),this.indeterminateChange.emit(!1)}),this._checked=!this._checked,this._transitionCheckState(this._checked?It.Checked:It.Unchecked),this._emitChangeEvent()):(this.disabled&&this.disabledInteractive||!this.disabled&&e==="noop")&&(this._inputElement.nativeElement.checked=this.checked,this._inputElement.nativeElement.indeterminate=this.indeterminate)}_onInteractionEvent(e){e.stopPropagation()}_onBlur(){Promise.resolve().then(()=>{this._onTouched(),this._changeDetectorRef.markForCheck()})}_getAnimationClassForCheckStateTransition(e,i){if(this._animationsDisabled)return"";switch(e){case It.Init:if(i===It.Checked)return this._animationClasses.uncheckedToChecked;if(i==It.Indeterminate)return this._checked?this._animationClasses.checkedToIndeterminate:this._animationClasses.uncheckedToIndeterminate;break;case It.Unchecked:return i===It.Checked?this._animationClasses.uncheckedToChecked:this._animationClasses.uncheckedToIndeterminate;case It.Checked:return i===It.Unchecked?this._animationClasses.checkedToUnchecked:this._animationClasses.checkedToIndeterminate;case It.Indeterminate:return i===It.Checked?this._animationClasses.indeterminateToChecked:this._animationClasses.indeterminateToUnchecked}return""}_syncIndeterminate(e){let i=this._inputElement;i&&(i.nativeElement.indeterminate=e)}_onInputClick(){this._handleInputClick()}_preventBubblingFromLabel(e){e.target&&this._inputElement&&e.target!==this._inputElement.nativeElement&&e.stopPropagation()}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-checkbox"]],viewQuery:function(i,r){if(i&1&&Ue(NL,5),i&2){let o;ce(o=de())&&(r._inputElement=o.first)}},hostAttrs:[1,"mat-mdc-checkbox"],hostVars:16,hostBindings:function(i,r){i&2&&(Et("id",r.id),re("tabindex",null)("aria-label",null)("aria-labelledby",null),wt(r.color?"mat-"+r.color:"mat-accent"),z("_mat-animation-noopable",r._animationsDisabled)("mdc-checkbox--disabled",r.disabled)("mat-mdc-checkbox-disabled",r.disabled)("mat-mdc-checkbox-checked",r.checked)("mat-mdc-checkbox-disabled-interactive",r.disabledInteractive))},inputs:{ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],ariaExpanded:[2,"aria-expanded","ariaExpanded",P],ariaControls:[0,"aria-controls","ariaControls"],ariaOwns:[0,"aria-owns","ariaOwns"],id:"id",required:[2,"required","required",P],labelPosition:"labelPosition",name:"name",value:"value",disableRipple:[2,"disableRipple","disableRipple",P],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?void 0:Yr(e)],color:"color",disabledInteractive:[2,"disabledInteractive","disabledInteractive",P],checked:[2,"checked","checked",P],disabled:[2,"disabled","disabled",P],indeterminate:[2,"indeterminate","indeterminate",P]},outputs:{change:"change",indeterminateChange:"indeterminateChange"},exportAs:["matCheckbox"],features:[Be([{provide:Ll,useExisting:Pt(()=>t),multi:!0},{provide:Vl,useExisting:t,multi:!0}]),at],ngContentSelectors:kL,decls:15,vars:23,consts:[["checkbox",""],["input",""],["label",""],["mat-internal-form-field","",3,"click","labelPosition","for"],[1,"mdc-checkbox"],["aria-hidden","true",1,"mat-mdc-checkbox-touch-target"],["type","checkbox",1,"mdc-checkbox__native-control",3,"blur","click","change","checked","indeterminate","disabled","id","required","tabIndex"],["aria-hidden","true",1,"mdc-checkbox__ripple"],["aria-hidden","true",1,"mdc-checkbox__background"],["focusable","false","viewBox","0 0 24 24",1,"mdc-checkbox__checkmark"],["fill","none","d","M1.73,12.91 8.1,19.28 22.79,4.59",1,"mdc-checkbox__checkmark-path"],[1,"mdc-checkbox__mixedmark"],["mat-ripple","","aria-hidden","true",1,"mat-mdc-checkbox-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled","matRippleCentered"],[1,"mat-internal-form-field-label","mdc-label"]],template:function(i,r){if(i&1&&(Pe(),m(0,"label",3),D("click",function(s){return r._preventBubblingFromLabel(s)}),m(1,"span",4,0),ie(3,"span",5),m(4,"input",6,1),D("blur",function(){return r._onBlur()})("click",function(){return r._onInputClick()})("change",function(s){return r._onInteractionEvent(s)}),g(),ie(6,"span",7),m(7,"span",8),Fi(),m(8,"svg",9),ie(9,"path",10),g(),id(),ie(10,"span",11),g(),ie(11,"span",12),g(),m(12,"span",13,2),he(14),g()()),i&2){let o=ht(2);U("labelPosition",r.labelPosition)("for",r.inputId),_(4),z("mdc-checkbox--selected",r.checked),U("checked",r.checked)("indeterminate",r.indeterminate)("disabled",r.disabled&&!r.disabledInteractive)("id",r.inputId)("required",r.required)("tabIndex",r.disabled&&!r.disabledInteractive?-1:r.tabIndex),re("aria-label",r.ariaLabel||null)("aria-labelledby",r.ariaLabelledby)("aria-describedby",r.ariaDescribedby)("aria-checked",r.indeterminate?"mixed":null)("aria-controls",r.ariaControls)("aria-disabled",r.disabled&&r.disabledInteractive?!0:null)("aria-expanded",r.ariaExpanded)("aria-owns",r.ariaOwns)("name",r.name)("value",r.value),_(7),U("matRippleTrigger",o)("matRippleDisabled",r.disableRipple||r.disabled)("matRippleCentered",!0)}},dependencies:[sr,Vw],styles:[`.mdc-checkbox {
  display: inline-block;
  position: relative;
  flex: 0 0 18px;
  box-sizing: content-box;
  width: 18px;
  height: 18px;
  line-height: 0;
  white-space: nowrap;
  cursor: pointer;
  vertical-align: bottom;
  padding: calc((var(--%NS%mat-checkbox-state-layer-size, 40px) - 18px) / 2);
  margin: calc((var(--%NS%mat-checkbox-state-layer-size, 40px) - var(--%NS%mat-checkbox-state-layer-size, 40px)) / 2);
}
.mdc-checkbox:hover > .mdc-checkbox__ripple {
  opacity: var(--%NS%mat-checkbox-unselected-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
  background-color: var(--%NS%mat-checkbox-unselected-hover-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mdc-checkbox:hover > .mat-mdc-checkbox-ripple > .mat-ripple-element {
  background-color: var(--%NS%mat-checkbox-unselected-hover-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mdc-checkbox .mdc-checkbox__native-control:focus + .mdc-checkbox__ripple {
  opacity: var(--%NS%mat-checkbox-unselected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
  background-color: var(--%NS%mat-checkbox-unselected-focus-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mdc-checkbox .mdc-checkbox__native-control:focus ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--%NS%mat-checkbox-unselected-focus-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mdc-checkbox:active > .mdc-checkbox__native-control + .mdc-checkbox__ripple {
  opacity: var(--%NS%mat-checkbox-unselected-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
  background-color: var(--%NS%mat-checkbox-unselected-pressed-state-layer-color, var(--%NS%mat-sys-primary));
}
.mdc-checkbox:active > .mdc-checkbox__native-control ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--%NS%mat-checkbox-unselected-pressed-state-layer-color, var(--%NS%mat-sys-primary));
}
.mdc-checkbox:hover > .mdc-checkbox__native-control:checked + .mdc-checkbox__ripple {
  opacity: var(--%NS%mat-checkbox-selected-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
  background-color: var(--%NS%mat-checkbox-selected-hover-state-layer-color, var(--%NS%mat-sys-primary));
}
.mdc-checkbox:hover > .mdc-checkbox__native-control:checked ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--%NS%mat-checkbox-selected-hover-state-layer-color, var(--%NS%mat-sys-primary));
}
.mdc-checkbox .mdc-checkbox__native-control:focus:checked + .mdc-checkbox__ripple {
  opacity: var(--%NS%mat-checkbox-selected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
  background-color: var(--%NS%mat-checkbox-selected-focus-state-layer-color, var(--%NS%mat-sys-primary));
}
.mdc-checkbox .mdc-checkbox__native-control:focus:checked ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--%NS%mat-checkbox-selected-focus-state-layer-color, var(--%NS%mat-sys-primary));
}
.mdc-checkbox:active > .mdc-checkbox__native-control:checked + .mdc-checkbox__ripple {
  opacity: var(--%NS%mat-checkbox-selected-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
  background-color: var(--%NS%mat-checkbox-selected-pressed-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mdc-checkbox:active > .mdc-checkbox__native-control:checked ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--%NS%mat-checkbox-selected-pressed-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control ~ .mat-mdc-checkbox-ripple .mat-ripple-element,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control + .mdc-checkbox__ripple {
  background-color: var(--%NS%mat-checkbox-unselected-hover-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mdc-checkbox .mdc-checkbox__native-control {
  position: absolute;
  margin: 0;
  padding: 0;
  opacity: 0;
  cursor: inherit;
  z-index: 1;
  width: var(--%NS%mat-checkbox-state-layer-size, 40px);
  height: var(--%NS%mat-checkbox-state-layer-size, 40px);
  top: calc((var(--%NS%mat-checkbox-state-layer-size, 40px) - var(--%NS%mat-checkbox-state-layer-size, 40px)) / 2);
  right: calc((var(--%NS%mat-checkbox-state-layer-size, 40px) - var(--%NS%mat-checkbox-state-layer-size, 40px)) / 2);
  left: calc((var(--%NS%mat-checkbox-state-layer-size, 40px) - var(--%NS%mat-checkbox-state-layer-size, 40px)) / 2);
}

.mdc-checkbox--disabled {
  cursor: default;
  pointer-events: none;
}

.mdc-checkbox__background {
  display: inline-flex;
  position: absolute;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  width: 18px;
  height: 18px;
  border: 2px solid currentColor;
  border-radius: 2px;
  background-color: transparent;
  pointer-events: none;
  will-change: background-color, border-color;
  transition: background-color 90ms cubic-bezier(0.4, 0, 0.6, 1), border-color 90ms cubic-bezier(0.4, 0, 0.6, 1);
  -webkit-print-color-adjust: exact;
  color-adjust: exact;
  border-color: var(--%NS%mat-checkbox-unselected-icon-color, var(--%NS%mat-sys-on-surface-variant));
  top: calc((var(--%NS%mat-checkbox-state-layer-size, 40px) - 18px) / 2);
  left: calc((var(--%NS%mat-checkbox-state-layer-size, 40px) - 18px) / 2);
}

.mdc-checkbox__native-control:enabled:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:enabled:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--%NS%mat-checkbox-selected-icon-color, var(--%NS%mat-sys-primary));
  background-color: var(--%NS%mat-checkbox-selected-icon-color, var(--%NS%mat-sys-primary));
}

.mdc-checkbox--disabled .mdc-checkbox__background {
  border-color: var(--%NS%mat-checkbox-disabled-unselected-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled .mdc-checkbox__background {
    border-color: GrayText;
  }
}

.mdc-checkbox__native-control:disabled:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:disabled:indeterminate ~ .mdc-checkbox__background {
  background-color: var(--%NS%mat-checkbox-disabled-selected-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  border-color: transparent;
}
@media (forced-colors: active) {
  .mdc-checkbox__native-control:disabled:checked ~ .mdc-checkbox__background,
  .mdc-checkbox__native-control:disabled:indeterminate ~ .mdc-checkbox__background {
    border-color: GrayText;
  }
}

.mdc-checkbox:hover > .mdc-checkbox__native-control:not(:checked) ~ .mdc-checkbox__background,
.mdc-checkbox:hover > .mdc-checkbox__native-control:not(:indeterminate) ~ .mdc-checkbox__background {
  border-color: var(--%NS%mat-checkbox-unselected-hover-icon-color, var(--%NS%mat-sys-on-surface));
  background-color: transparent;
}

.mdc-checkbox:hover > .mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mdc-checkbox:hover > .mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--%NS%mat-checkbox-selected-hover-icon-color, var(--%NS%mat-sys-primary));
  background-color: var(--%NS%mat-checkbox-selected-hover-icon-color, var(--%NS%mat-sys-primary));
}

.mdc-checkbox__native-control:focus:focus:not(:checked) ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:focus:focus:not(:indeterminate) ~ .mdc-checkbox__background {
  border-color: var(--%NS%mat-checkbox-unselected-focus-icon-color, var(--%NS%mat-sys-on-surface));
}

.mdc-checkbox__native-control:focus:focus:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:focus:focus:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--%NS%mat-checkbox-selected-focus-icon-color, var(--%NS%mat-sys-primary));
  background-color: var(--%NS%mat-checkbox-selected-focus-icon-color, var(--%NS%mat-sys-primary));
}

.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox:hover > .mdc-checkbox__native-control ~ .mdc-checkbox__background,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control:focus ~ .mdc-checkbox__background,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__background {
  border-color: var(--%NS%mat-checkbox-disabled-unselected-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox:hover > .mdc-checkbox__native-control ~ .mdc-checkbox__background,
  .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control:focus ~ .mdc-checkbox__background,
  .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__background {
    border-color: GrayText;
  }
}
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  background-color: var(--%NS%mat-checkbox-disabled-selected-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  border-color: transparent;
}

.mdc-checkbox__checkmark {
  position: absolute;
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  width: 100%;
  opacity: 0;
  transition: opacity 180ms cubic-bezier(0.4, 0, 0.6, 1);
  color: var(--%NS%mat-checkbox-selected-checkmark-color, var(--%NS%mat-sys-on-primary));
}
@media (forced-colors: active) {
  .mdc-checkbox__checkmark {
    color: CanvasText;
  }
}

.mdc-checkbox--disabled .mdc-checkbox__checkmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__checkmark {
  color: var(--%NS%mat-checkbox-disabled-selected-checkmark-color, var(--%NS%mat-sys-surface));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled .mdc-checkbox__checkmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__checkmark {
    color: GrayText;
  }
}

.mdc-checkbox__checkmark-path {
  transition: stroke-dashoffset 180ms cubic-bezier(0.4, 0, 0.6, 1);
  stroke: currentColor;
  stroke-width: 3.12px;
  stroke-dashoffset: 29.7833385;
  stroke-dasharray: 29.7833385;
}

.mdc-checkbox__mixedmark {
  width: 100%;
  height: 0;
  transform: scaleX(0) rotate(0deg);
  border-width: 1px;
  border-style: solid;
  opacity: 0;
  transition: opacity 90ms cubic-bezier(0.4, 0, 0.6, 1), transform 90ms cubic-bezier(0.4, 0, 0.6, 1);
  border-color: var(--%NS%mat-checkbox-selected-checkmark-color, var(--%NS%mat-sys-on-primary));
}
@media (forced-colors: active) {
  .mdc-checkbox__mixedmark {
    margin: 0 1px;
  }
}

.mdc-checkbox--disabled .mdc-checkbox__mixedmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__mixedmark {
  border-color: var(--%NS%mat-checkbox-disabled-selected-checkmark-color, var(--%NS%mat-sys-surface));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled .mdc-checkbox__mixedmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__mixedmark {
    border-color: GrayText;
  }
}

.mdc-checkbox--anim-unchecked-checked .mdc-checkbox__background,
.mdc-checkbox--anim-unchecked-indeterminate .mdc-checkbox__background,
.mdc-checkbox--anim-checked-unchecked .mdc-checkbox__background,
.mdc-checkbox--anim-indeterminate-unchecked .mdc-checkbox__background {
  animation-duration: 180ms;
  animation-timing-function: linear;
}

.mdc-checkbox--anim-unchecked-checked .mdc-checkbox__checkmark-path {
  animation: mdc-checkbox-unchecked-checked-checkmark-path 180ms linear;
  transition: none;
}

.mdc-checkbox--anim-unchecked-indeterminate .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-unchecked-indeterminate-mixedmark 90ms linear;
  transition: none;
}

.mdc-checkbox--anim-checked-unchecked .mdc-checkbox__checkmark-path {
  animation: mdc-checkbox-checked-unchecked-checkmark-path 90ms linear;
  transition: none;
}

.mdc-checkbox--anim-checked-indeterminate .mdc-checkbox__checkmark {
  animation: mdc-checkbox-checked-indeterminate-checkmark 90ms linear;
  transition: none;
}
.mdc-checkbox--anim-checked-indeterminate .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-checked-indeterminate-mixedmark 90ms linear;
  transition: none;
}

.mdc-checkbox--anim-indeterminate-checked .mdc-checkbox__checkmark {
  animation: mdc-checkbox-indeterminate-checked-checkmark 500ms linear;
  transition: none;
}
.mdc-checkbox--anim-indeterminate-checked .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-indeterminate-checked-mixedmark 500ms linear;
  transition: none;
}

.mdc-checkbox--anim-indeterminate-unchecked .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-indeterminate-unchecked-mixedmark 300ms linear;
  transition: none;
}

.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 1), background-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path,
.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path {
  stroke-dashoffset: 0;
}

.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__checkmark {
  transition: opacity 180ms cubic-bezier(0, 0, 0.2, 1), transform 180ms cubic-bezier(0, 0, 0.2, 1);
  opacity: 1;
}
.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transform: scaleX(1) rotate(-45deg);
}

.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__checkmark {
  transform: rotate(45deg);
  opacity: 0;
  transition: opacity 90ms cubic-bezier(0.4, 0, 0.6, 1), transform 90ms cubic-bezier(0.4, 0, 0.6, 1);
}
.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transform: scaleX(1) rotate(0deg);
  opacity: 1;
}

@keyframes mdc-checkbox-unchecked-checked-checkmark-path {
  0%, 50% {
    stroke-dashoffset: 29.7833385;
  }
  50% {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
  }
  100% {
    stroke-dashoffset: 0;
  }
}
@keyframes mdc-checkbox-unchecked-indeterminate-mixedmark {
  0%, 68.2% {
    transform: scaleX(0);
  }
  68.2% {
    animation-timing-function: cubic-bezier(0, 0, 0, 1);
  }
  100% {
    transform: scaleX(1);
  }
}
@keyframes mdc-checkbox-checked-unchecked-checkmark-path {
  from {
    animation-timing-function: cubic-bezier(0.4, 0, 1, 1);
    opacity: 1;
    stroke-dashoffset: 0;
  }
  to {
    opacity: 0;
    stroke-dashoffset: -29.7833385;
  }
}
@keyframes mdc-checkbox-checked-indeterminate-checkmark {
  from {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
    transform: rotate(0deg);
    opacity: 1;
  }
  to {
    transform: rotate(45deg);
    opacity: 0;
  }
}
@keyframes mdc-checkbox-indeterminate-checked-checkmark {
  from {
    animation-timing-function: cubic-bezier(0.14, 0, 0, 1);
    transform: rotate(45deg);
    opacity: 0;
  }
  to {
    transform: rotate(360deg);
    opacity: 1;
  }
}
@keyframes mdc-checkbox-checked-indeterminate-mixedmark {
  from {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
    transform: rotate(-45deg);
    opacity: 0;
  }
  to {
    transform: rotate(0deg);
    opacity: 1;
  }
}
@keyframes mdc-checkbox-indeterminate-checked-mixedmark {
  from {
    animation-timing-function: cubic-bezier(0.14, 0, 0, 1);
    transform: rotate(0deg);
    opacity: 1;
  }
  to {
    transform: rotate(315deg);
    opacity: 0;
  }
}
@keyframes mdc-checkbox-indeterminate-unchecked-mixedmark {
  0% {
    animation-timing-function: linear;
    transform: scaleX(1);
    opacity: 1;
  }
  32.8%, 100% {
    transform: scaleX(0);
    opacity: 0;
  }
}
.mat-mdc-checkbox {
  display: inline-block;
  position: relative;
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mat-mdc-checkbox-touch-target,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__native-control,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__ripple,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mat-mdc-checkbox-ripple::before,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__checkmark,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-checkbox label {
  cursor: pointer;
}
.mat-mdc-checkbox .mat-internal-form-field {
  color: var(--%NS%mat-checkbox-label-text-color, var(--%NS%mat-sys-on-surface));
  font-family: var(--%NS%mat-checkbox-label-text-font, var(--%NS%mat-sys-body-medium-font));
  line-height: var(--%NS%mat-checkbox-label-text-line-height, var(--%NS%mat-sys-body-medium-line-height));
  font-size: var(--%NS%mat-checkbox-label-text-size, var(--%NS%mat-sys-body-medium-size));
  letter-spacing: var(--%NS%mat-checkbox-label-text-tracking, var(--%NS%mat-sys-body-medium-tracking));
  font-weight: var(--%NS%mat-checkbox-label-text-weight, var(--%NS%mat-sys-body-medium-weight));
}
.mat-mdc-checkbox.mat-mdc-checkbox-disabled.mat-mdc-checkbox-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-checkbox.mat-mdc-checkbox-disabled.mat-mdc-checkbox-disabled-interactive input {
  cursor: default;
}
.mat-mdc-checkbox.mat-mdc-checkbox-disabled label {
  cursor: default;
}
.mat-mdc-checkbox.mat-mdc-checkbox-disabled .mat-internal-form-field-label {
  color: var(--%NS%mat-checkbox-disabled-label-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mat-mdc-checkbox.mat-mdc-checkbox-disabled .mat-internal-form-field-label {
    color: GrayText;
  }
}
.mat-mdc-checkbox .mat-internal-form-field-label:empty {
  display: none;
}
.mat-mdc-checkbox .mdc-checkbox__ripple {
  opacity: 0;
}

.mat-mdc-checkbox .mat-mdc-checkbox-ripple,
.mdc-checkbox__ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
}
.mat-mdc-checkbox .mat-mdc-checkbox-ripple:not(:empty),
.mdc-checkbox__ripple:not(:empty) {
  transform: translateZ(0);
}

.mat-mdc-checkbox-ripple .mat-ripple-element {
  opacity: 0.1;
}

.mat-mdc-checkbox-touch-target {
  position: absolute;
  top: 50%;
  left: 50%;
  height: var(--%NS%mat-checkbox-touch-target-size, 48px);
  width: var(--%NS%mat-checkbox-touch-target-size, 48px);
  transform: translate(-50%, -50%);
  display: var(--%NS%mat-checkbox-touch-target-display, block);
}

.mat-mdc-checkbox .mat-mdc-checkbox-ripple::before {
  border-radius: 50%;
}

.mdc-checkbox__native-control:focus-visible ~ .mat-focus-indicator::before {
  content: "";
}
`],encapsulation:2})}return t})(),Bw=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({imports:[Uv,ve]})}return t})();function OL(t,n){if(t&1){let e=Le();m(0,"button",15),D("click",function(){ee(e);let r=w(2);return te(r.onEditText())}),m(1,"mat-icon"),y(2,"edit"),g(),y(3," Editar texto "),g()}}function PL(t,n){if(t&1){let e=Le();m(0,"mat-form-field",16)(1,"mat-label"),y(2,"Tamanho da fonte (pontos)"),g(),m(3,"input",17),D("input",function(r){ee(e);let o=w(2);return te(o.onFontSizeChange(r.target.valueAsNumber))}),g()(),m(4,"mat-checkbox",18),D("change",function(r){ee(e);let o=w(2);return te(o.onBoldChange(r.checked))}),y(5," Negrito "),g(),m(6,"p",19),y(7,"Alinhamento horizontal do texto"),g(),m(8,"mat-button-toggle-group",20),D("change",function(r){ee(e);let o=w(2);return te(o.onAlignChange(r.value))}),m(9,"mat-button-toggle",21)(10,"mat-icon"),y(11,"format_align_left"),g()(),m(12,"mat-button-toggle",22)(13,"mat-icon"),y(14,"format_align_center"),g()(),m(15,"mat-button-toggle",23)(16,"mat-icon"),y(17,"format_align_right"),g()()(),m(18,"p",24),y(19,"Alinhamento vertical do texto"),g(),m(20,"mat-button-toggle-group",25),D("change",function(r){ee(e);let o=w(2);return te(o.onVerticalAlignChange(r.value))}),m(21,"mat-button-toggle",26)(22,"mat-icon"),y(23,"vertical_align_top"),g()(),m(24,"mat-button-toggle",27)(25,"mat-icon"),y(26,"vertical_align_center"),g()(),m(27,"mat-button-toggle",28)(28,"mat-icon"),y(29,"vertical_align_bottom"),g()()(),m(30,"mat-form-field",16)(31,"mat-label"),y(32,"Girar (graus)"),g(),m(33,"mat-select",29),D("selectionChange",function(r){ee(e);let o=w(2);return te(o.onRotationChange(r.value))}),m(34,"mat-option",30),y(35,"0\xB0 (normal)"),g(),m(36,"mat-option",30),y(37,"90\xB0 (vertical, lendo de cima pra baixo)"),g(),m(38,"mat-option",30),y(39,"180\xB0 (de cabe\xE7a pra baixo)"),g(),m(40,"mat-option",30),y(41,"270\xB0 (vertical, lendo de baixo pra cima)"),g()()()}if(t&2){let e=n;_(3),U("value",e.fontSizeDots),_(),U("checked",e.bold),_(4),U("value",e.align),_(12),U("value",e.verticalAlign),_(13),U("value",e.rotationDeg),_(),U("value",0),_(2),U("value",90),_(2),U("value",180),_(2),U("value",270)}}function FL(t,n){t&1&&(m(0,"p",4),y(1,"Imagem selecionada."),g())}function LL(t,n){if(t&1){let e=Le();m(0,"div",0),V(1,OL,4,0,"button",1),m(2,"button",2),D("click",function(){ee(e);let r=w();return te(r.onDuplicate())}),m(3,"mat-icon"),y(4,"content_copy"),g(),y(5," Duplicar "),g(),m(6,"button",3),D("click",function(){ee(e);let r=w();return te(r.onDelete())}),m(7,"mat-icon"),y(8,"delete"),g(),y(9," Remover "),g()(),m(10,"p",4),y(11," Atalhos: duplo clique na caixa tamb\xE9m edita. Ctrl+C copia, Ctrl+V cola, Delete remove. "),g(),ie(12,"mat-divider"),V(13,PL,42,9)(14,FL,2,0,"p",4),ie(15,"mat-divider"),m(16,"h3"),y(17,"Posi\xE7\xE3o e tamanho"),g(),m(18,"p",5),y(19,"Empurrar a caixa \xB7 1 dot por toque"),g(),m(20,"div",6)(21,"button",7),D("click",function(){ee(e);let r=w();return te(r.onNudge(-1,0))}),m(22,"mat-icon"),y(23,"arrow_back"),g()(),m(24,"button",8),D("click",function(){ee(e);let r=w();return te(r.onNudge(0,-1))}),m(25,"mat-icon"),y(26,"arrow_upward"),g()(),m(27,"button",9),D("click",function(){ee(e);let r=w();return te(r.onNudge(0,1))}),m(28,"mat-icon"),y(29,"arrow_downward"),g()(),m(30,"button",10),D("click",function(){ee(e);let r=w();return te(r.onNudge(1,0))}),m(31,"mat-icon"),y(32,"arrow_forward"),g()()(),m(33,"p",4),y(34," Digitar direto resolve o caso da caixa que sumiu da \xE1rea vis\xEDvel: ponha 0 e 0 que ela volta. "),g(),m(35,"div",11)(36,"mat-form-field",12)(37,"mat-label"),y(38,"X (dots)"),g(),m(39,"input",13),D("input",function(r){ee(e);let o=w();return te(o.onXChange(r.target.valueAsNumber))}),g()(),m(40,"mat-form-field",12)(41,"mat-label"),y(42,"Y (dots)"),g(),m(43,"input",13),D("input",function(r){ee(e);let o=w();return te(o.onYChange(r.target.valueAsNumber))}),g()()(),m(44,"div",11)(45,"mat-form-field",12)(46,"mat-label"),y(47,"Largura (dots)"),g(),m(48,"input",14),D("input",function(r){ee(e);let o=w();return te(o.onWidthChange(r.target.valueAsNumber))}),g()(),m(49,"mat-form-field",12)(50,"mat-label"),y(51,"Altura (dots)"),g(),m(52,"input",14),D("input",function(r){ee(e);let o=w();return te(o.onHeightChange(r.target.valueAsNumber))}),g()()()}if(t&2){let e,i=n,r=w();_(),B(r.selectedTextElement()?1:-1),_(12),B((e=r.selectedTextElement())?13:14,e),_(26),U("value",i.xDots),_(4),U("value",i.yDots),_(5),U("value",i.widthDots),_(4),U("value",i.heightDots)}}var Fs=class t{store=d(Nn);selectedElement=Q(()=>{let n=this.store.selectedId();return this.store.layout().elements.find(e=>e.id===n)??null});selectedTextElement=Q(()=>{let n=this.selectedElement();return n?.kind==="text"?n:null});onEditText(){let n=this.selectedTextElement();n&&this.store.startEditing(n.id)}onDuplicate(){this.store.duplicateSelected()}onDelete(){let n=this.store.selectedId();n&&this.store.removeElement(n)}onNudge(n,e){let i=this.store.selectedId();i&&this.store.nudgeElement(i,n,e)}onXChange(n){let e=this.selectedElement();e&&this.store.updateElement(e.id,{xDots:Math.round(n)})}onYChange(n){let e=this.selectedElement();e&&this.store.updateElement(e.id,{yDots:Math.round(n)})}onWidthChange(n){let e=this.selectedElement();e&&this.store.resizeElement(e.id,n,e.heightDots)}onHeightChange(n){let e=this.selectedElement();e&&this.store.resizeElement(e.id,e.widthDots,n)}onFontSizeChange(n){let e=this.store.selectedId();e&&this.store.setTextFontSize(e,n)}onBoldChange(n){let e=this.store.selectedId();e&&this.store.updateElement(e,{bold:n})}onAlignChange(n){let e=this.store.selectedId();e&&this.store.updateElement(e,{align:n})}onVerticalAlignChange(n){let e=this.store.selectedId();e&&this.store.updateElement(e,{verticalAlign:n})}onRotationChange(n){let e=this.store.selectedId();e&&this.store.updateElement(e,{rotationDeg:n})}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=T({type:t,selectors:[["app-label-properties-panel"]],decls:1,vars:1,consts:[[1,"properties-actions"],["mat-flat-button","","type","button"],["mat-stroked-button","","type","button",3,"click"],["mat-stroked-button","","color","warn","type","button",3,"click"],[1,"properties-hint"],[1,"properties-label"],[1,"nudge-pad"],["mat-stroked-button","","type","button","aria-label","Empurrar pra esquerda",3,"click"],["mat-stroked-button","","type","button","aria-label","Empurrar pra cima",3,"click"],["mat-stroked-button","","type","button","aria-label","Empurrar pra baixo",3,"click"],["mat-stroked-button","","type","button","aria-label","Empurrar pra direita",3,"click"],[1,"size-fields"],["appearance","outline"],["matInput","","type","number",3,"input","value"],["matInput","","type","number","min","8",3,"input","value"],["mat-flat-button","","type","button",3,"click"],["appearance","outline",1,"full-width"],["matInput","","type","number","min","8","max","999",3,"input","value"],[3,"change","checked"],["id","rotulo-alinhamento-h",1,"properties-label"],["hideSingleSelectionIndicator","","aria-labelledby","rotulo-alinhamento-h",1,"align-toggle",3,"change","value"],["value","left","aria-label","Alinhar \xE0 esquerda"],["value","center","aria-label","Centralizar"],["value","right","aria-label","Alinhar \xE0 direita"],["id","rotulo-alinhamento-v",1,"properties-label"],["hideSingleSelectionIndicator","","aria-labelledby","rotulo-alinhamento-v",1,"align-toggle",3,"change","value"],["value","top","aria-label","Alinhar ao topo"],["value","middle","aria-label","Centralizar na vertical"],["value","bottom","aria-label","Alinhar \xE0 base"],[3,"selectionChange","value"],[3,"value"]],template:function(e,i){if(e&1&&V(0,LL,53,6),e&2){let r;B((r=i.selectedElement())?0:-1,r)}},dependencies:[Mn,lr,Lw,Bv,Pf,Bw,Uv,af,sf,Tn,cr,Di,Zn,Yn,Nf,Tf,xf,wf,hr],styles:["[_nghost-%COMP%]{display:flex;flex-direction:column;gap:.5rem}h3[_ngcontent-%COMP%]{margin:0 0 .25rem;font-size:.9375rem}.full-width[_ngcontent-%COMP%]{width:100%}.properties-hint[_ngcontent-%COMP%]{font-size:.8125rem;color:var(--%NS%mat-sys-on-surface-variant, #616161);text-align:center;margin:0}.properties-label[_ngcontent-%COMP%]{font-size:.75rem;font-weight:500;color:var(--%NS%mat-sys-on-surface-variant, #616161);margin:.5rem 0 .125rem}.properties-actions[_ngcontent-%COMP%]{display:flex;flex-wrap:wrap;gap:.5rem}.properties-actions[_ngcontent-%COMP%]   button[_ngcontent-%COMP%]{flex:1 1 auto;min-width:7rem}.align-toggle[_ngcontent-%COMP%]{display:flex}.align-toggle[_ngcontent-%COMP%]     .mat-button-toggle{flex:1}.nudge-pad[_ngcontent-%COMP%]{display:flex;gap:.375rem}.nudge-pad[_ngcontent-%COMP%]   button[_ngcontent-%COMP%]{flex:1;min-width:0;padding:0}.size-fields[_ngcontent-%COMP%]{display:flex;gap:.5rem}.size-fields[_ngcontent-%COMP%]   mat-form-field[_ngcontent-%COMP%]{flex:1;min-width:0}"]})};var Ff=class t{store=d(Nn);sheetRef=d(ys);data=d(av);host=d(O);title=Q(()=>{let n=this.store.selectedId(),e=this.store.layout().elements.find(r=>r.id===n);if(!e)return"Propriedades";if(e.kind==="image")return"Imagem";let i=e.text.trim();return i||"Texto vazio"});constructor(){Zr(n=>{let i=this.host.nativeElement.closest("mat-bottom-sheet-container")??this.host.nativeElement,r=()=>this.data.onHeightChange(i.getBoundingClientRect().height),o=new ResizeObserver(r);o.observe(i),r(),n(()=>{o.disconnect(),this.data.onHeightChange(0)})})}close(){this.store.select(null),this.sheetRef.dismiss()}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=T({type:t,selectors:[["app-label-properties-sheet"]],decls:7,vars:1,consts:[[1,"sheet-head"],[1,"sheet-title"],["mat-icon-button","","type","button","aria-label","Fechar propriedades",3,"click"]],template:function(e,i){e&1&&(m(0,"header",0)(1,"span",1),y(2),g(),m(3,"button",2),D("click",function(){return i.close()}),m(4,"mat-icon"),y(5,"close"),g()()(),ie(6,"app-label-properties-panel")),e&2&&(_(2),Te(i.title()))},dependencies:[Mn,bo,Zn,Yn,Fs],styles:["[_nghost-%COMP%]{display:block}.sheet-head[_ngcontent-%COMP%]{display:flex;align-items:center;justify-content:space-between;gap:.5rem;margin-bottom:.25rem}.sheet-title[_ngcontent-%COMP%]{font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}"]})};var VL=["stageContainer"],Hw=(t,n)=>n.key,Lf=(t,n)=>n.id;function BL(t,n){if(t&1&&(m(0,"header",7),ie(1,"app-printer-status-badge",29),m(2,"button",30),y(3),m(4,"mat-icon",31),y(5,"expand_more"),g()()()),t&2){let e=w(),i=ht(8);_(2),U("matMenuTriggerFor",i),_(),At(" ",e.store.layout().sizePreset.label," ")}}function jL(t,n){if(t&1&&(m(0,"mat-option",36),y(1),g()),t&2){let e=n.$implicit;U("value",e.key),_(),Te(e.label)}}function HL(t,n){if(t&1){let e=Le();m(0,"header",32)(1,"h1"),y(2,"Editor de etiquetas"),g(),ie(3,"app-printer-status-badge"),g(),m(4,"div",33)(5,"mat-form-field",34)(6,"mat-label"),y(7,"Tamanho da etiqueta"),g(),m(8,"mat-select",35),D("selectionChange",function(r){ee(e);let o=w();return te(o.onSizePresetChange(r.value))}),Xt(9,jL,2,2,"mat-option",36,Hw),g()(),m(11,"button",37),D("click",function(){ee(e);let r=w();return te(r.onAddText())}),m(12,"mat-icon"),y(13,"title"),g(),y(14," Adicionar texto "),g(),m(15,"button",37),D("click",function(){ee(e),w();let r=ht(4);return te(r.click())}),m(16,"mat-icon"),y(17,"image"),g(),y(18," Adicionar imagem "),g(),m(19,"button",38)(20,"mat-icon"),y(21,"folder"),g(),y(22," Etiquetas salvas "),g(),m(23,"button",39),D("click",function(){ee(e);let r=w();return te(r.onPrint())}),m(24,"mat-icon"),y(25,"print"),g(),y(26),g()()}if(t&2){let e=w(),i=ht(17);_(8),U("value",e.store.layout().sizePreset.key),_(),Kt(e.sizePresets),_(10),U("matMenuTriggerFor",i),_(4),U("disabled",e.printing()),_(3),At(" ",e.printing()?"Imprimindo\u2026":"Imprimir"," ")}}function UL(t,n){if(t&1){let e=Le();m(0,"button",12),D("click",function(){let r=ee(e).$implicit,o=w();return te(o.onSizePresetChange(r.key))}),y(1),g()}if(t&2){let e=n.$implicit;_(),At(" ",e.label," ")}}function zL(t,n){t&1&&(m(0,"button",11),y(1,"Nenhum elemento ainda"),g())}function $L(t,n){if(t&1){let e=Le();m(0,"button",12),D("click",function(){let r=ee(e).$implicit,o=w();return te(o.store.select(r.id))}),m(1,"mat-icon"),y(2),g(),y(3),g()}if(t&2){let e=n.$implicit,i=w();_(2),Te(e.kind==="text"?"title":"image"),_(),At(" ",i.elementPreviewLabel(e)," ")}}function GL(t,n){if(t&1){let e=Le();m(0,"div",40)(1,"button",41),D("click",function(){let r=ee(e).$implicit,o=w(2);return te(o.onLoadFromLibrary(r.id))}),m(2,"span",42),y(3),g()(),m(4,"button",43),D("click",function(r){let o=ee(e).$implicit,s=w(2);return te(s.onDeleteFromLibrary(o.id,o.name,r))}),m(5,"mat-icon"),y(6,"delete"),g()()()}if(t&2){let e=n.$implicit;_(3),Te(e.name),_(),re("aria-label","Remover etiqueta salva "+e.name)}}function WL(t,n){if(t&1&&(ie(0,"mat-divider"),Xt(1,GL,7,2,"div",40,Lf)),t&2){let e=w();_(),Kt(e.library.savedLabels())}}function qL(t,n){if(t&1&&ie(0,"app-label-element-box",18),t&2){let e=n.$implicit,i=w();U("element",e)("scale",i.scale())}}function YL(t,n){if(t&1&&ie(0,"div",44),t&2){let e=n.$implicit,i=w();Jt("left",e*i.scale(),"px")}}function ZL(t,n){if(t&1&&ie(0,"div",45),t&2){let e=n.$implicit,i=w();Jt("top",e*i.scale(),"px")}}function XL(t,n){t&1&&(m(0,"p",21),y(1," Etiqueta vazia. Comece por "),m(2,"strong"),y(3,"Adicionar texto"),g(),y(4,". "),g())}function KL(t,n){t&1&&(m(0,"p",26),y(1," Arraste pra posicionar. As caixas grudam nas bordas/centro de outras caixas e da pr\xF3pria etiqueta. "),g())}function QL(t,n){if(t&1){let e=Le();m(0,"button",50),D("click",function(){let r=ee(e).$implicit,o=w(3);return te(o.store.select(r.id))}),m(1,"mat-icon"),y(2),g(),m(3,"span",51),y(4),g()()}if(t&2){let e=n.$implicit,i=w(3);z("is-selected",i.store.selectedId()===e.id),_(2),Te(e.kind==="text"?"title":"image"),_(2),Te(i.elementPreviewLabel(e))}}function JL(t,n){if(t&1&&(m(0,"mat-card",46)(1,"mat-card-content")(2,"h2"),y(3,"Elementos na etiqueta"),g(),m(4,"p",48),y(5," Selecione por aqui quando uma caixa ficar dif\xEDcil de achar ou clicar na etiqueta (muito pequena, vazia ou fora da \xE1rea vis\xEDvel). "),g(),Xt(6,QL,5,4,"button",49,Lf),g()()),t&2){let e=w(2);_(6),Kt(e.store.layout().elements)}}function eV(t,n){t&1&&(m(0,"mat-card",47)(1,"mat-card-content")(2,"h2"),y(3,"Propriedades"),g(),ie(4,"app-label-properties-panel"),g()())}function tV(t,n){if(t&1&&(m(0,"div",27),V(1,JL,8,0,"mat-card",46),V(2,eV,5,0,"mat-card",47),g()),t&2){let e=w();_(),B(e.store.layout().elements.length>0?1:-1),_(),B(e.hasSelection()?2:-1)}}function nV(t,n){if(t&1){let e=Le();m(0,"nav",28)(1,"button",52),D("click",function(){ee(e);let r=w();return te(r.onAddText())}),m(2,"mat-icon"),y(3,"title"),g(),m(4,"span"),y(5,"Texto"),g()(),m(6,"button",52),D("click",function(){ee(e),w();let r=ht(4);return te(r.click())}),m(7,"mat-icon"),y(8,"image"),g(),m(9,"span"),y(10,"Imagem"),g()(),m(11,"button",53)(12,"mat-icon"),y(13,"layers"),g(),m(14,"span"),y(15,"Elementos"),g()(),m(16,"button",53)(17,"mat-icon"),y(18,"folder"),g(),m(19,"span"),y(20,"Salvas"),g()(),m(21,"button",54),D("click",function(){ee(e);let r=w();return te(r.onPrint())}),m(22,"mat-icon"),y(23,"print"),g(),y(24),g()()}if(t&2){let e=w(),i=ht(12),r=ht(17);_(11),U("matMenuTriggerFor",i),_(5),U("matMenuTriggerFor",r),_(5),U("disabled",e.printing()),_(3),At(" ",e.printing()?"Imprimindo\u2026":"Imprimir"," ")}}var jw=24,iV=4,rV="(max-width: 900px)",Vf=class t{store=d(Nn);library=d(If);printLabelLayout=d(Mf);printerConnection=d(mr);snackBar=d(pw);bottomSheet=d(UD);dialog=d(Es);injector=d(R);destroyRef=d(We);sizePresets=cl;printing=j(!1);MIN_ZOOM=1;MAX_ZOOM=4;stageContainerRef=ui("stageContainer");containerWidthPx=j(400);containerHeightBudgetPx=j(400);resizeObserver=null;isCompact=j(!1);sheetHeightPx=j(0);zoom=j(1);sheetRef=null;stageWidthDots=Q(()=>Ki(this.store.layout().sizePreset));stageHeightDots=Q(()=>Qi(this.store.layout().sizePreset));scale=Q(()=>{let n=Math.max(50,this.containerWidthPx()-jw),e=Math.max(50,this.containerHeightBudgetPx()-jw),i=Math.min(n/this.stageWidthDots(),e/this.stageHeightDots());return Math.min(i,iV)*this.zoom()});stageWidthPx=Q(()=>this.stageWidthDots()*this.scale());stageHeightPx=Q(()=>this.stageHeightDots()*this.scale());zoomLabel=Q(()=>`${Math.round(this.zoom()*100)}%`);hasSelection=Q(()=>this.store.selectedId()!==null);constructor(){this.watchCompactQuery(),Zr(n=>{let e=this.stageContainerRef()?.nativeElement;e&&(this.resizeObserver=new ResizeObserver(()=>this.measureStageContainer()),this.resizeObserver.observe(e),this.measureStageContainer(),n(()=>this.resizeObserver?.disconnect()))}),ut(()=>{let n=this.isCompact()&&this.hasSelection();Ne(()=>n?this.openPropertiesSheet():this.closePropertiesSheet())})}watchCompactQuery(){if(typeof window>"u"||!window.matchMedia)return;let n=window.matchMedia(rV);this.isCompact.set(n.matches);let e=i=>this.isCompact.set(i.matches);n.addEventListener("change",e),this.destroyRef.onDestroy(()=>n.removeEventListener("change",e))}openPropertiesSheet(){if(this.sheetRef)return;let n={onHeightChange:e=>this.sheetHeightPx.set(e)};this.sheetRef=this.bottomSheet.open(Ff,{hasBackdrop:!1,panelClass:"properties-bottom-sheet",injector:this.injector,data:n}),this.sheetRef.afterDismissed().subscribe(()=>{this.sheetRef=null,this.sheetHeightPx.set(0),this.store.select(null)})}closePropertiesSheet(){this.sheetRef?.dismiss()}measureStageContainer(){let n=this.stageContainerRef()?.nativeElement;if(!n)return;let e=n.getBoundingClientRect();this.containerWidthPx.set(e.width),e.height>=1&&this.containerHeightBudgetPx.set(e.height)}onZoom(n){let e=Math.round((this.zoom()+n*.25)*100)/100;this.zoom.set(Math.min(this.MAX_ZOOM,Math.max(this.MIN_ZOOM,e)))}onResetZoom(){this.zoom.set(1)}onSizePresetChange(n){this.store.setSizePresetKey(n)}onStageBackgroundClick(){this.store.select(null)}elementPreviewLabel(n){if(n.kind==="image")return"Imagem";let e=n.text.trim();return e||"Texto vazio"}onAddText(){this.store.addTextElement()}onImageSelected(n){let e=n.target,i=e.files?.[0];if(!i)return;let r=new FileReader;r.onload=()=>{let o=r.result,s=new Image;s.onload=()=>{this.store.addImageElement(o,s.naturalWidth,s.naturalHeight)},s.src=o},r.readAsDataURL(i),e.value=""}onKeyDown(n){let e=n.target;if(e.tagName==="TEXTAREA"||e.tagName==="INPUT")return;let i=this.store.selectedId();if(n.key==="Delete"||n.key==="Backspace"){i&&(this.store.removeElement(i),n.preventDefault());return}if(i&&n.key.startsWith("Arrow")){let o=n.shiftKey?10:1,s=n.key==="ArrowLeft"?-o:n.key==="ArrowRight"?o:0,a=n.key==="ArrowUp"?-o:n.key==="ArrowDown"?o:0;(s||a)&&(this.store.nudgeElement(i,s,a),n.preventDefault());return}if(!(n.ctrlKey||n.metaKey))return;let r=n.key.toLowerCase();r==="c"&&i?(this.store.copySelected(),n.preventDefault()):r==="v"&&this.store.hasClipboard()&&(this.store.pasteClipboard(),n.preventDefault())}async onPrint(){if(this.store.layout().elements.length===0){this.snackBar.open("Adicione algo na etiqueta antes de imprimir.","Fechar",{duration:3e3});return}let n=this.printerConnection.device();if(this.printerConnection.status()!=="connected"||!n){this.snackBar.open("Nenhuma impressora alcan\xE7\xE1vel neste endere\xE7o.","Configurar",{duration:6e3}).onAction().subscribe(()=>this.dialog.open(Os));return}this.printing.set(!0);try{await this.printLabelLayout.execute(this.store.layout(),n),this.snackBar.open("Etiqueta enviada para a impressora.","Fechar",{duration:3e3})}catch(e){let i=e instanceof Error?e.message:"Falha ao imprimir.";this.snackBar.open(i,"Fechar",{duration:5e3})}finally{this.printing.set(!1)}}onSaveToLibrary(){let n=this.store.layout();if(n.elements.length===0){this.snackBar.open("Adicione algo na etiqueta antes de salvar.","Fechar",{duration:3e3});return}let e=window.prompt("Nome da etiqueta:","");if(!e)return;let i=this.library.save(e,n);this.snackBar.open(i?`Etiqueta "${e}" salva.`:"N\xE3o foi poss\xEDvel salvar (armazenamento local cheio ou indispon\xEDvel).","Fechar",{duration:3e3})}onLoadFromLibrary(n){let e=this.library.savedLabels().find(i=>i.id===n);e&&this.store.loadLayout(this.library.toLayout(e))}onDeleteFromLibrary(n,e,i){i.stopPropagation(),window.confirm(`Remover a etiqueta salva "${e}"?`)&&this.library.remove(n)}onExportLibrary(){if(this.library.savedLabels().length===0){this.snackBar.open("Nenhuma etiqueta salva pra exportar.","Fechar",{duration:3e3});return}this.library.exportAll()}async onImportLibraryFile(n){let e=n.target,i=e.files?.[0];if(i)try{let r=await this.library.importFromFile(i);this.snackBar.open(`${r} etiqueta(s) importada(s).`,"Fechar",{duration:3e3})}catch(r){let o=r instanceof Ul?r.message:"Falha ao importar o arquivo.";this.snackBar.open(o,"Fechar",{duration:4e3})}finally{e.value=""}}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=T({type:t,selectors:[["app-label-designer-page"]],viewQuery:function(e,i){e&1&&$i(i.stageContainerRef,VL,5),e&2&&Gi()},hostBindings:function(e,i){e&1&&D("keydown",function(o){return i.onKeyDown(o)},fp)},features:[Be([Nn])],decls:57,vars:19,consts:[["imageInput",""],["importInput",""],["sizeMenu","matMenu"],["elementsMenu","matMenu"],["libraryMenu","matMenu"],["stageContainer",""],[1,"designer-layout"],[1,"compact-bar"],["type","file","accept","image/*","hidden","",3,"change"],["type","file","accept","application/json","hidden","",3,"change"],["mat-menu-item","","type","button"],["mat-menu-item","","type","button","disabled",""],["mat-menu-item","","type","button",3,"click"],[1,"designer-columns"],[1,"stage-card"],[1,"stage-card-content"],[1,"stage-container"],[1,"stage",3,"pointerdown"],[3,"element","scale"],[1,"guide","guide-vertical",3,"left"],[1,"guide","guide-horizontal",3,"top"],[1,"stage-empty"],[1,"zoom-controls"],["mat-icon-button","","type","button","aria-label","Diminuir o zoom",3,"click","disabled"],["mat-button","","type","button",1,"zoom-value",3,"click","disabled"],["mat-icon-button","","type","button","aria-label","Aumentar o zoom",3,"click","disabled"],[1,"stage-hint"],[1,"side-panels"],["aria-label","A\xE7\xF5es da etiqueta",1,"action-bar"],["compact",""],["mat-stroked-button","","type","button",1,"size-pill",3,"matMenuTriggerFor"],["iconPositionEnd",""],[1,"designer-header"],[1,"toolbar"],["appearance","outline",1,"size-field"],[3,"selectionChange","value"],[3,"value"],["mat-stroked-button","","type","button",3,"click"],["mat-stroked-button","","type","button",3,"matMenuTriggerFor"],["mat-flat-button","","color","primary","type","button",1,"print-button",3,"click","disabled"],[1,"saved-label-row"],["mat-menu-item","","type","button",1,"saved-label-item",3,"click"],[1,"saved-label-name"],["mat-menu-item","","type","button",1,"saved-label-delete",3,"click"],[1,"guide","guide-vertical"],[1,"guide","guide-horizontal"],[1,"elements-card"],[1,"properties-card"],[1,"properties-hint"],["type","button",1,"element-list-item",3,"is-selected"],["type","button",1,"element-list-item",3,"click"],[1,"element-list-label"],["mat-button","","type","button",1,"action",3,"click"],["mat-button","","type","button",1,"action",3,"matMenuTriggerFor"],["mat-flat-button","","color","primary","type","button",1,"print-fab",3,"click","disabled"]],template:function(e,i){if(e&1){let r=Le();m(0,"div",6),V(1,BL,6,2,"header",7)(2,HL,27,4),m(3,"input",8,0),D("change",function(s){return i.onImageSelected(s)}),g(),m(5,"input",9,1),D("change",function(s){return i.onImportLibraryFile(s)}),g(),m(7,"mat-menu",null,2),Xt(9,UL,2,1,"button",10,Hw),g(),m(11,"mat-menu",null,3),V(13,zL,2,0,"button",11),Xt(14,$L,4,2,"button",10,Lf),g(),m(16,"mat-menu",null,4)(18,"button",12),D("click",function(){return i.onSaveToLibrary()}),m(19,"mat-icon"),y(20,"save"),g(),y(21," Salvar etiqueta atual\u2026 "),g(),V(22,WL,3,0),ie(23,"mat-divider"),m(24,"button",12),D("click",function(){return i.onExportLibrary()}),m(25,"mat-icon"),y(26,"download"),g(),y(27," Exportar tudo (.json) "),g(),m(28,"button",12),D("click",function(){ee(r);let s=ht(6);return te(s.click())}),m(29,"mat-icon"),y(30,"upload"),g(),y(31," Importar (.json) "),g()(),m(32,"div",13)(33,"mat-card",14)(34,"mat-card-content",15)(35,"div",16,5)(37,"div",17),D("pointerdown",function(){return i.onStageBackgroundClick()}),Xt(38,qL,1,2,"app-label-element-box",18,Lf),Xt(40,YL,1,2,"div",19,Gd),Xt(42,ZL,1,2,"div",20,Gd),g(),V(44,XL,5,0,"p",21),g(),m(45,"div",22)(46,"button",23),D("click",function(){return i.onZoom(-1)}),m(47,"mat-icon"),y(48,"remove"),g()(),m(49,"button",24),D("click",function(){return i.onResetZoom()}),y(50),g(),m(51,"button",25),D("click",function(){return i.onZoom(1)}),m(52,"mat-icon"),y(53,"add"),g()()(),V(54,KL,2,0,"p",26),g()(),V(55,tV,3,2,"div",27),g(),V(56,nV,25,4,"nav",28),g()}e&2&&(Jt("--%NS%sheet-height",i.sheetHeightPx(),"px"),z("is-compact",i.isCompact()),_(),B(i.isCompact()?1:2),_(8),Kt(i.sizePresets),_(4),B(i.store.layout().elements.length===0?13:-1),_(),Kt(i.store.layout().elements),_(8),B(i.library.savedLabels().length>0?22:-1),_(15),Jt("width",i.stageWidthPx(),"px")("height",i.stageHeightPx(),"px"),_(),Kt(i.store.layout().elements),_(2),Kt(i.store.activeGuides().vertical),_(2),Kt(i.store.activeGuides().horizontal),_(2),B(i.store.layout().elements.length===0?44:-1),_(2),U("disabled",i.zoom()<=i.MIN_ZOOM),_(3),U("disabled",i.zoom()===1),_(),At(" ",i.zoomLabel()," "),_(),U("disabled",i.zoom()>=i.MAX_ZOOM),_(3),B(i.isCompact()?-1:54),_(),B(i.isCompact()?-1:55),_(),B(i.isCompact()?56:-1))},dependencies:[Mn,lr,bo,oE,iE,rE,af,sf,Tn,cr,Di,Zn,Yn,FE,xs,kl,PE,xf,wf,hr,Rf,Fs,Af],styles:["[_nghost-%COMP%]{display:flex;flex-direction:column;flex:1 1 auto;min-height:0}.designer-layout[_ngcontent-%COMP%]{display:flex;flex-direction:column;flex:1 1 auto;min-height:0;gap:1.5rem;padding:1.5rem;max-width:1800px;width:100%;margin:0 auto;box-sizing:border-box}@media(max-width:900px){.designer-layout[_ngcontent-%COMP%]{padding:.75rem;gap:.75rem}}@media(max-height:500px)and (orientation:landscape){.designer-layout[_ngcontent-%COMP%]{padding:.5rem .75rem;gap:.5rem}}.designer-header[_ngcontent-%COMP%]{flex:0 0 auto;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:1rem}.designer-header[_ngcontent-%COMP%]   h1[_ngcontent-%COMP%]{margin:0;font-size:1.5rem}@media(max-width:900px){.designer-header[_ngcontent-%COMP%]   h1[_ngcontent-%COMP%]{font-size:1.25rem}}@media(max-height:500px)and (orientation:landscape){.designer-header[_ngcontent-%COMP%]   h1[_ngcontent-%COMP%]{font-size:1.0625rem}}.toolbar[_ngcontent-%COMP%]{flex:0 0 auto;display:flex;align-items:flex-start;flex-wrap:wrap;gap:.75rem}.size-field[_ngcontent-%COMP%]{min-width:220px}.print-button[_ngcontent-%COMP%]{margin-left:auto}.designer-columns[_ngcontent-%COMP%]{display:flex;flex:1 1 auto;min-height:0;gap:1.5rem}@media(max-width:900px){.designer-columns[_ngcontent-%COMP%]{flex-direction:column}}.stage-card[_ngcontent-%COMP%]{display:flex;flex-direction:column;flex:1 1 0;min-width:0;min-height:0}@media(max-width:900px){.stage-card[_ngcontent-%COMP%]{min-height:min(45dvh,420px)}}@media(max-height:500px)and (orientation:landscape){.stage-card[_ngcontent-%COMP%]{min-height:60dvh}}.stage-card-content[_ngcontent-%COMP%]{display:flex;flex-direction:column;align-items:center;flex:1 1 auto;min-height:0;position:relative}.compact-bar[_ngcontent-%COMP%]{flex:0 0 auto;display:flex;align-items:center;gap:.5rem;min-width:0}.compact-bar[_ngcontent-%COMP%]   app-printer-status-badge[_ngcontent-%COMP%]{min-width:0;flex:1 1 auto}.size-pill[_ngcontent-%COMP%]{flex:0 0 auto;white-space:nowrap}.action-bar[_ngcontent-%COMP%]{flex:0 0 auto;display:flex;align-items:center;gap:.25rem}.action-bar[_ngcontent-%COMP%]   .action[_ngcontent-%COMP%]{flex:1 1 0;min-width:0;display:inline-flex;flex-direction:column;align-items:center;justify-content:center;gap:.125rem;min-height:52px;padding:0 .125rem;line-height:1.1}.action-bar[_ngcontent-%COMP%]   .action[_ngcontent-%COMP%]   span[_ngcontent-%COMP%]{font-size:.6875rem}.action-bar[_ngcontent-%COMP%]   .action[_ngcontent-%COMP%]     .mat-mdc-button-touch-target{height:100%}.action-bar[_ngcontent-%COMP%]   .print-fab[_ngcontent-%COMP%]{flex:0 0 auto;min-height:52px;border-radius:26px;padding:0 1rem}.zoom-controls[_ngcontent-%COMP%]{position:absolute;top:.5rem;right:.5rem;z-index:5;display:flex;align-items:center;gap:.125rem;padding:0 .25rem;border-radius:20px;background:var(--%NS%mat-sys-surface-container-high, #fff);box-shadow:0 1px 4px #0003}.zoom-controls[_ngcontent-%COMP%]   .zoom-value[_ngcontent-%COMP%]{min-width:3.5rem;font-variant-numeric:tabular-nums}.stage-empty[_ngcontent-%COMP%]{position:absolute;left:50%;top:50%;transform:translate(-50%,-50%);margin:0;padding:0 1rem;text-align:center;font-size:.875rem;color:var(--%NS%mat-sys-on-surface-variant, #616161);pointer-events:none}.stage-container[_ngcontent-%COMP%]{width:100%;display:flex;justify-content:center;align-items:center;position:relative;margin-bottom:var(--%NS%sheet-height, 0px);transition:margin-bottom .18s ease;overflow:auto}@media(prefers-reduced-motion:reduce){.stage-container[_ngcontent-%COMP%]{transition:none}}.stage-container[_ngcontent-%COMP%]{flex:1 1 auto;min-height:0}.stage[_ngcontent-%COMP%]{position:relative;flex:0 0 auto;margin:auto;background:#fff;border:1px solid var(--%NS%mat-sys-outline, #9e9e9e);overflow:visible}.stage-hint[_ngcontent-%COMP%], .properties-hint[_ngcontent-%COMP%]{font-size:.8125rem;color:var(--%NS%mat-sys-on-surface-variant, #616161);text-align:center}.guide[_ngcontent-%COMP%]{position:absolute;background:#e91e63;pointer-events:none;z-index:10}.guide-vertical[_ngcontent-%COMP%]{top:0;bottom:0;width:1px}.guide-horizontal[_ngcontent-%COMP%]{left:0;right:0;height:1px}.properties-card[_ngcontent-%COMP%]   mat-card-content[_ngcontent-%COMP%], .elements-card[_ngcontent-%COMP%]   mat-card-content[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:.5rem}.side-panels[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:1.5rem;flex:0 1 380px;min-width:320px;min-height:0;overflow-y:auto}@media(max-width:900px){.side-panels[_ngcontent-%COMP%]{min-width:0;overflow-y:visible}}.element-list-item[_ngcontent-%COMP%]{display:flex;align-items:center;gap:.5rem;width:100%;padding:.5rem .75rem;border:1px solid var(--%NS%mat-sys-outline, #9e9e9e);border-radius:4px;background:none;cursor:pointer;font:inherit;text-align:left}.element-list-item.is-selected[_ngcontent-%COMP%]{border-color:#3f51b5;background:color-mix(in srgb,#3f51b5 8%,transparent)}.element-list-label[_ngcontent-%COMP%]{flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.saved-label-row[_ngcontent-%COMP%]{display:flex;align-items:center}.saved-label-item[_ngcontent-%COMP%]{display:flex;align-items:center;gap:.5rem;flex:1;min-width:0}.saved-label-name[_ngcontent-%COMP%]{flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;text-align:left}.saved-label-delete[_ngcontent-%COMP%]{flex:0 0 auto;width:48px;min-width:48px;padding:0;justify-content:center;color:var(--%NS%mat-sys-error, #c62828)}.saved-label-delete[_ngcontent-%COMP%]   .mat-icon[_ngcontent-%COMP%]{margin-right:0}.saved-label-delete[_ngcontent-%COMP%]:hover{opacity:.8}"]})};var Uw=[{path:"",component:Vf},{path:"designer",redirectTo:"",pathMatch:"full"}];var zw={providers:[om(),Fg(Uw),{provide:ds,useClass:Vu},{provide:Nu,useClass:Fu}]};var Bf=class t{static \u0275fac=function(e){return new(e||t)};static \u0275cmp=T({type:t,selectors:[["app-root"]],decls:2,vars:0,consts:[[1,"app-content"]],template:function(e,i){e&1&&(m(0,"div",0),ie(1,"router-outlet"),g())},dependencies:[ol],styles:["[_nghost-%COMP%]{display:flex;flex-direction:column;height:100dvh}.app-content[_ngcontent-%COMP%]{display:flex;flex-direction:column;flex:1 1 auto;min-height:0}router-outlet[_ngcontent-%COMP%]{display:none}"]})};eg(Bf,zw).catch(t=>console.error(t));
