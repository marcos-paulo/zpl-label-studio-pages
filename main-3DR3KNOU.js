var tw=Object.defineProperty,nw=Object.defineProperties;var iw=Object.getOwnPropertyDescriptors;var Xv=Object.getOwnPropertySymbols;var rw=Object.prototype.hasOwnProperty,ow=Object.prototype.propertyIsEnumerable;var Kv=(t,n,e)=>n in t?tw(t,n,{enumerable:!0,configurable:!0,writable:!0,value:e}):t[n]=e,v=(t,n)=>{for(var e in n||={})rw.call(n,e)&&Kv(t,e,n[e]);if(Xv)for(var e of Xv(n))ow.call(n,e)&&Kv(t,e,n[e]);return t},N=(t,n)=>nw(t,iw(n));var Mt=null,Ql=!1,gr=1,sw=null,et=Symbol("SIGNAL");function Q(t){let n=Mt;return Mt=t,n}function nc(){return Mt}var vr={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function br(t){if(Ql)throw new Error("");if(Mt===null)return;Mt.consumerOnSignalRead(t);let n=Mt.producersTail;if(n!==void 0&&n.producer===t)return;let e,i=Mt.recomputing;if(i&&(e=n!==void 0?n.nextProducer:Mt.producers,e!==void 0&&e.producer===t)){Mt.producersTail=e,e.lastReadVersion=t.version,e.knownValidAtEpoch=gr;return}let r=t.consumersTail;if(r!==void 0&&r.consumer===Mt&&(!i||r.knownValidAtEpoch===gr))return;let o=Io(Mt),s={producer:t,consumer:Mt,nextProducer:e,prevConsumer:void 0,knownValidAtEpoch:gr,lastReadVersion:t.version,nextConsumer:void 0};Mt.producersTail=s,n!==void 0?n.nextProducer=s:Mt.producers=s,o&&tb(t,s)}function Qv(){gr++}function ic(t){if(!(Io(t)&&!t.dirty)&&!(!t.dirty&&t.lastCleanEpoch===gr)){if(!t.producerMustRecompute(t)&&!wo(t)){tc(t);return}t.producerRecomputeValue(t),tc(t)}}function Yf(t){if(t.consumers===void 0)return;let n=Ql;Ql=!0;try{for(let e=t.consumers;e!==void 0;e=e.nextConsumer){let i=e.consumer;i.dirty||aw(i)}}finally{Ql=n}}function Zf(){return Mt?.consumerAllowSignalWrites!==!1}function aw(t){t.dirty=!0,Yf(t),t.consumerMarkedDirty?.(t)}function tc(t){t.dirty=!1,t.lastCleanEpoch=gr}function Ti(t){return t&&Jv(t),Q(t)}function Jv(t){if(t.producersTail?.knownValidAtEpoch===gr){let n=t.producers;for(;n!==void 0;)n.knownValidAtEpoch=null,n=n.nextProducer}t.producersTail=void 0,t.recomputing=!0}function _r(t,n){Q(n),t&&eb(t)}function eb(t){t.recomputing=!1;let n=t.producersTail,e=n!==void 0?n.nextProducer:t.producers;if(e!==void 0){if(Io(t))do e=Xf(e);while(e!==void 0);n!==void 0?n.nextProducer=void 0:t.producers=void 0}}function wo(t){for(let n=t.producers;n!==void 0;n=n.nextProducer){let e=n.producer,i=n.lastReadVersion;if(i!==e.version||(ic(e),i!==e.version))return!0}return!1}function Ni(t){if(Io(t)){let n=t.producers;for(;n!==void 0;)n=Xf(n)}t.producers=void 0,t.producersTail=void 0,t.consumers=void 0,t.consumersTail=void 0}function tb(t,n){let e=t.consumersTail,i=Io(t);if(e!==void 0?(n.nextConsumer=e.nextConsumer,e.nextConsumer=n):(n.nextConsumer=void 0,t.consumers=n),n.prevConsumer=e,t.consumersTail=n,!i)for(let r=t.producers;r!==void 0;r=r.nextProducer)tb(r.producer,r)}function Xf(t){let n=t.producer,e=t.nextProducer,i=t.nextConsumer,r=t.prevConsumer;if(t.nextConsumer=void 0,t.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:n.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(n.consumers=i,!Io(n)){let o=n.producers;for(;o!==void 0;)o=Xf(o)}return e}function Io(t){return t.consumerIsAlwaysLive||t.consumers!==void 0}function rc(t){sw?.(t)}function oc(t,n){return Object.is(t,n)}function Xs(t,n){let e=Object.create(lw);e.computation=t,n!==void 0&&(e.equal=n);let i=()=>{if(ic(e),br(e),e.value===Zs)throw e.error;return e.value};return i[et]=e,rc(e),i}var Jl=Symbol("UNSET"),ec=Symbol("COMPUTING"),Zs=Symbol("ERRORED"),lw=N(v({},vr),{value:Jl,dirty:!0,error:null,equal:oc,kind:"computed",producerMustRecompute(t){return t.value===Jl||t.value===ec},producerRecomputeValue(t){if(t.value===ec)throw new Error("");let n=t.value;t.value=ec;let e=Ti(t),i,r=!1;try{i=t.computation(),Q(null),r=n!==Jl&&n!==Zs&&i!==Zs&&t.equal(n,i)}catch(o){i=Zs,t.error=o}finally{_r(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function cw(){throw new Error}var nb=cw;function ib(t){nb(t)}function Kf(t){nb=t}var dw=null;function Qf(t,n){let e=Object.create(Mo);e.value=t,n!==void 0&&(e.equal=n);let i=()=>rb(e);return i[et]=e,rc(e),[i,s=>yr(e,s),s=>Jf(e,s)]}function rb(t){return br(t),t.value}function yr(t,n){Zf()||ib(t),t.equal(t.value,n)||(t.value=n,uw(t))}function Jf(t,n){Zf()||ib(t),yr(t,n(t.value))}var Mo=N(v({},vr),{equal:oc,value:void 0,kind:"signal"});function uw(t){t.version++,Qv(),Yf(t),dw?.(t)}var eh=N(v({},vr),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function th(t){if(t.dirty=!1,t.version>0&&!wo(t))return;t.version++;let n=Ti(t);try{t.cleanup(),t.fn()}finally{_r(t,n)}}var nh;function sc(){return nh}function Rn(t){let n=nh;return nh=t,n}var ob=Symbol("NotFound");function To(t){return t===ob||t?.name==="\u0275NotFound"}function sb(t){let n=Q(null);try{return t()}finally{Q(n)}}function le(t){return typeof t=="function"}function No(t){let e=t(i=>{Error.call(i),i.stack=new Error().stack});return e.prototype=Object.create(Error.prototype),e.prototype.constructor=e,e}var ac=No(t=>function(e){t(this),this.message=e?`${e.length} errors occurred during unsubscription:
${e.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=e});function Sr(t,n){if(t){let e=t.indexOf(n);0<=e&&t.splice(e,1)}}var pe=class t{constructor(n){this.initialTeardown=n,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let n;if(!this.closed){this.closed=!0;let{_parentage:e}=this;if(e)if(this._parentage=null,Array.isArray(e))for(let o of e)o.remove(this);else e.remove(this);let{initialTeardown:i}=this;if(le(i))try{i()}catch(o){n=o instanceof ac?o.errors:[o]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let o of r)try{ab(o)}catch(s){n=n??[],s instanceof ac?n=[...n,...s.errors]:n.push(s)}}if(n)throw new ac(n)}}add(n){var e;if(n&&n!==this)if(this.closed)ab(n);else{if(n instanceof t){if(n.closed||n._hasParent(this))return;n._addParent(this)}(this._finalizers=(e=this._finalizers)!==null&&e!==void 0?e:[]).push(n)}}_hasParent(n){let{_parentage:e}=this;return e===n||Array.isArray(e)&&e.includes(n)}_addParent(n){let{_parentage:e}=this;this._parentage=Array.isArray(e)?(e.push(n),e):e?[e,n]:n}_removeParent(n){let{_parentage:e}=this;e===n?this._parentage=null:Array.isArray(e)&&Sr(e,n)}remove(n){let{_finalizers:e}=this;e&&Sr(e,n),n instanceof t&&n._removeParent(this)}};pe.EMPTY=(()=>{let t=new pe;return t.closed=!0,t})();var ih=pe.EMPTY;function lc(t){return t instanceof pe||t&&"closed"in t&&le(t.remove)&&le(t.add)&&le(t.unsubscribe)}function ab(t){le(t)?t():t.unsubscribe()}var fn={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var ko={setTimeout(t,n,...e){let{delegate:i}=ko;return i?.setTimeout?i.setTimeout(t,n,...e):setTimeout(t,n,...e)},clearTimeout(t){let{delegate:n}=ko;return(n?.clearTimeout||clearTimeout)(t)},delegate:void 0};function cc(t){ko.setTimeout(()=>{let{onUnhandledError:n}=fn;if(n)n(t);else throw t})}function Ks(){}var lb=rh("C",void 0,void 0);function cb(t){return rh("E",void 0,t)}function db(t){return rh("N",t,void 0)}function rh(t,n,e){return{kind:t,value:n,error:e}}var Cr=null;function Ao(t){if(fn.useDeprecatedSynchronousErrorHandling){let n=!Cr;if(n&&(Cr={errorThrown:!1,error:null}),t(),n){let{errorThrown:e,error:i}=Cr;if(Cr=null,e)throw i}}else t()}function ub(t){fn.useDeprecatedSynchronousErrorHandling&&Cr&&(Cr.errorThrown=!0,Cr.error=t)}var Dr=class extends pe{constructor(n){super(),this.isStopped=!1,n?(this.destination=n,lc(n)&&n.add(this)):this.destination=mw}static create(n,e,i){return new Xn(n,e,i)}next(n){this.isStopped?sh(db(n),this):this._next(n)}error(n){this.isStopped?sh(cb(n),this):(this.isStopped=!0,this._error(n))}complete(){this.isStopped?sh(lb,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(n){this.destination.next(n)}_error(n){try{this.destination.error(n)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},fw=Function.prototype.bind;function oh(t,n){return fw.call(t,n)}var ah=class{constructor(n){this.partialObserver=n}next(n){let{partialObserver:e}=this;if(e.next)try{e.next(n)}catch(i){dc(i)}}error(n){let{partialObserver:e}=this;if(e.error)try{e.error(n)}catch(i){dc(i)}else dc(n)}complete(){let{partialObserver:n}=this;if(n.complete)try{n.complete()}catch(e){dc(e)}}},Xn=class extends Dr{constructor(n,e,i){super();let r;if(le(n)||!n)r={next:n??void 0,error:e??void 0,complete:i??void 0};else{let o;this&&fn.useDeprecatedNextContext?(o=Object.create(n),o.unsubscribe=()=>this.unsubscribe(),r={next:n.next&&oh(n.next,o),error:n.error&&oh(n.error,o),complete:n.complete&&oh(n.complete,o)}):r=n}this.destination=new ah(r)}};function dc(t){fn.useDeprecatedSynchronousErrorHandling?ub(t):cc(t)}function hw(t){throw t}function sh(t,n){let{onStoppedNotification:e}=fn;e&&ko.setTimeout(()=>e(t,n))}var mw={closed:!0,next:Ks,error:hw,complete:Ks};var Ro=typeof Symbol=="function"&&Symbol.observable||"@@observable";function Lt(t){return t}function uc(...t){return lh(t)}function lh(t){return t.length===0?Lt:t.length===1?t[0]:function(e){return t.reduce((i,r)=>r(i),e)}}var ne=class t{constructor(n){n&&(this._subscribe=n)}lift(n){let e=new t;return e.source=this,e.operator=n,e}subscribe(n,e,i){let r=gw(n)?n:new Xn(n,e,i);return Ao(()=>{let{operator:o,source:s}=this;r.add(o?o.call(r,s):s?this._subscribe(r):this._trySubscribe(r))}),r}_trySubscribe(n){try{return this._subscribe(n)}catch(e){n.error(e)}}forEach(n,e){return e=fb(e),new e((i,r)=>{let o=new Xn({next:s=>{try{n(s)}catch(a){r(a),o.unsubscribe()}},error:r,complete:i});this.subscribe(o)})}_subscribe(n){var e;return(e=this.source)===null||e===void 0?void 0:e.subscribe(n)}[Ro](){return this}pipe(...n){return lh(n)(this)}toPromise(n){return n=fb(n),new n((e,i)=>{let r;this.subscribe(o=>r=o,o=>i(o),()=>e(r))})}};ne.create=t=>new ne(t);function fb(t){var n;return(n=t??fn.Promise)!==null&&n!==void 0?n:Promise}function pw(t){return t&&le(t.next)&&le(t.error)&&le(t.complete)}function gw(t){return t&&t instanceof Dr||pw(t)&&lc(t)}function vw(t){return le(t?.lift)}function ue(t){return n=>{if(vw(n))return n.lift(function(e){try{return t(e,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function fe(t,n,e,i,r){return new ch(t,n,e,i,r)}var ch=class extends Dr{constructor(n,e,i,r,o,s){super(n),this.onFinalize=o,this.shouldUnsubscribe=s,this._next=e?function(a){try{e(a)}catch(l){n.error(l)}}:super._next,this._error=r?function(a){try{r(a)}catch(l){n.error(l)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(a){n.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var n;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:e}=this;super.unsubscribe(),!e&&((n=this.onFinalize)===null||n===void 0||n.call(this))}}};var hb=No(t=>function(){t(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var x=class extends ne{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(n){let e=new fc(this,this);return e.operator=n,e}_throwIfClosed(){if(this.closed)throw new hb}next(n){Ao(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let e of this.currentObservers)e.next(n)}})}error(n){Ao(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=n;let{observers:e}=this;for(;e.length;)e.shift().error(n)}})}complete(){Ao(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:n}=this;for(;n.length;)n.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var n;return((n=this.observers)===null||n===void 0?void 0:n.length)>0}_trySubscribe(n){return this._throwIfClosed(),super._trySubscribe(n)}_subscribe(n){return this._throwIfClosed(),this._checkFinalizedStatuses(n),this._innerSubscribe(n)}_innerSubscribe(n){let{hasError:e,isStopped:i,observers:r}=this;return e||i?ih:(this.currentObservers=null,r.push(n),new pe(()=>{this.currentObservers=null,Sr(r,n)}))}_checkFinalizedStatuses(n){let{hasError:e,thrownError:i,isStopped:r}=this;e?n.error(i):r&&n.complete()}asObservable(){let n=new ne;return n.source=this,n}};x.create=(t,n)=>new fc(t,n);var fc=class extends x{constructor(n,e){super(),this.destination=n,this.source=e}next(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.next)===null||i===void 0||i.call(e,n)}error(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.error)===null||i===void 0||i.call(e,n)}complete(){var n,e;(e=(n=this.destination)===null||n===void 0?void 0:n.complete)===null||e===void 0||e.call(n)}_subscribe(n){var e,i;return(i=(e=this.source)===null||e===void 0?void 0:e.subscribe(n))!==null&&i!==void 0?i:ih}};var tt=class extends x{constructor(n){super(),this._value=n}get value(){return this.getValue()}_subscribe(n){let e=super._subscribe(n);return!e.closed&&n.next(this._value),e}getValue(){let{hasError:n,thrownError:e,_value:i}=this;if(n)throw e;return this._throwIfClosed(),i}next(n){super.next(this._value=n)}};var Qs={now(){return(Qs.delegate||Date).now()},delegate:void 0};var ki=class extends x{constructor(n=1/0,e=1/0,i=Qs){super(),this._bufferSize=n,this._windowTime=e,this._timestampProvider=i,this._buffer=[],this._infiniteTimeWindow=!0,this._infiniteTimeWindow=e===1/0,this._bufferSize=Math.max(1,n),this._windowTime=Math.max(1,e)}next(n){let{isStopped:e,_buffer:i,_infiniteTimeWindow:r,_timestampProvider:o,_windowTime:s}=this;e||(i.push(n),!r&&i.push(o.now()+s)),this._trimBuffer(),super.next(n)}_subscribe(n){this._throwIfClosed(),this._trimBuffer();let e=this._innerSubscribe(n),{_infiniteTimeWindow:i,_buffer:r}=this,o=r.slice();for(let s=0;s<o.length&&!n.closed;s+=i?1:2)n.next(o[s]);return this._checkFinalizedStatuses(n),e}_trimBuffer(){let{_bufferSize:n,_timestampProvider:e,_buffer:i,_infiniteTimeWindow:r}=this,o=(r?1:2)*n;if(n<1/0&&o<i.length&&i.splice(0,i.length-o),!r){let s=e.now(),a=0;for(let l=1;l<i.length&&i[l]<=s;l+=2)a=l;a&&i.splice(0,a+1)}}};var hc=class extends pe{constructor(n,e){super()}schedule(n,e=0){return this}};var Js={setInterval(t,n,...e){let{delegate:i}=Js;return i?.setInterval?i.setInterval(t,n,...e):setInterval(t,n,...e)},clearInterval(t){let{delegate:n}=Js;return(n?.clearInterval||clearInterval)(t)},delegate:void 0};var mc=class extends hc{constructor(n,e){super(n,e),this.scheduler=n,this.work=e,this.pending=!1}schedule(n,e=0){var i;if(this.closed)return this;this.state=n;let r=this.id,o=this.scheduler;return r!=null&&(this.id=this.recycleAsyncId(o,r,e)),this.pending=!0,this.delay=e,this.id=(i=this.id)!==null&&i!==void 0?i:this.requestAsyncId(o,this.id,e),this}requestAsyncId(n,e,i=0){return Js.setInterval(n.flush.bind(n,this),i)}recycleAsyncId(n,e,i=0){if(i!=null&&this.delay===i&&this.pending===!1)return e;e!=null&&Js.clearInterval(e)}execute(n,e){if(this.closed)return new Error("executing a cancelled action");this.pending=!1;let i=this._execute(n,e);if(i)return i;this.pending===!1&&this.id!=null&&(this.id=this.recycleAsyncId(this.scheduler,this.id,null))}_execute(n,e){let i=!1,r;try{this.work(n)}catch(o){i=!0,r=o||new Error("Scheduled action threw falsy error")}if(i)return this.unsubscribe(),r}unsubscribe(){if(!this.closed){let{id:n,scheduler:e}=this,{actions:i}=e;this.work=this.state=this.scheduler=null,this.pending=!1,Sr(i,this),n!=null&&(this.id=this.recycleAsyncId(e,n,null)),this.delay=null,super.unsubscribe()}}};var dh=(()=>{class t{constructor(e,i=t.now){this.schedulerActionCtor=e,this.now=i}schedule(e,i=0,r){return new this.schedulerActionCtor(this,e).schedule(r,i)}}return t.now=Qs.now,t})();var pc=class extends dh{constructor(n,e=dh.now){super(n,e),this.actions=[],this._active=!1}flush(n){let{actions:e}=this;if(this._active){e.push(n);return}let i;this._active=!0;do if(i=n.execute(n.state,n.delay))break;while(n=e.shift());if(this._active=!1,i){for(;n=e.shift();)n.unsubscribe();throw i}}};var ea=new pc(mc),mb=ea;var We=new ne(t=>t.complete());function gc(t){return t&&le(t.schedule)}function uh(t){return t[t.length-1]}function vc(t){return le(uh(t))?t.pop():void 0}function On(t){return gc(uh(t))?t.pop():void 0}function pb(t,n){return typeof uh(t)=="number"?t.pop():n}function vb(t,n,e,i){function r(o){return o instanceof e?o:new e(function(s){s(o)})}return new(e||(e=Promise))(function(o,s){function a(d){try{c(i.next(d))}catch(f){s(f)}}function l(d){try{c(i.throw(d))}catch(f){s(f)}}function c(d){d.done?o(d.value):r(d.value).then(a,l)}c((i=i.apply(t,n||[])).next())})}function gb(t){var n=typeof Symbol=="function"&&Symbol.iterator,e=n&&t[n],i=0;if(e)return e.call(t);if(t&&typeof t.length=="number")return{next:function(){return t&&i>=t.length&&(t=void 0),{value:t&&t[i++],done:!t}}};throw new TypeError(n?"Object is not iterable.":"Symbol.iterator is not defined.")}function Er(t){return this instanceof Er?(this.v=t,this):new Er(t)}function bb(t,n,e){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=e.apply(t,n||[]),r,o=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",s),r[Symbol.asyncIterator]=function(){return this},r;function s(p){return function(S){return Promise.resolve(S).then(p,f)}}function a(p,S){i[p]&&(r[p]=function(E){return new Promise(function($,H){o.push([p,E,$,H])>1||l(p,E)})},S&&(r[p]=S(r[p])))}function l(p,S){try{c(i[p](S))}catch(E){h(o[0][3],E)}}function c(p){p.value instanceof Er?Promise.resolve(p.value.v).then(d,f):h(o[0][2],p)}function d(p){l("next",p)}function f(p){l("throw",p)}function h(p,S){p(S),o.shift(),o.length&&l(o[0][0],o[0][1])}}function _b(t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var n=t[Symbol.asyncIterator],e;return n?n.call(t):(t=typeof gb=="function"?gb(t):t[Symbol.iterator](),e={},i("next"),i("throw"),i("return"),e[Symbol.asyncIterator]=function(){return this},e);function i(o){e[o]=t[o]&&function(s){return new Promise(function(a,l){s=t[o](s),r(a,l,s.done,s.value)})}}function r(o,s,a,l){Promise.resolve(l).then(function(c){o({value:c,done:a})},s)}}var bc=(t=>t&&typeof t.length=="number"&&typeof t!="function");function _c(t){return le(t?.then)}function yc(t){return le(t[Ro])}function Sc(t){return Symbol.asyncIterator&&le(t?.[Symbol.asyncIterator])}function Cc(t){return new TypeError(`You provided ${t!==null&&typeof t=="object"?"an invalid object":`'${t}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function bw(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var Dc=bw();function Ec(t){return le(t?.[Dc])}function xc(t){return bb(this,arguments,function*(){let e=t.getReader();try{for(;;){let{value:i,done:r}=yield Er(e.read());if(r)return yield Er(void 0);yield yield Er(i)}}finally{e.releaseLock()}})}function wc(t){return le(t?.getReader)}function ke(t){if(t instanceof ne)return t;if(t!=null){if(yc(t))return _w(t);if(bc(t))return yw(t);if(_c(t))return Sw(t);if(Sc(t))return yb(t);if(Ec(t))return Cw(t);if(wc(t))return Dw(t)}throw Cc(t)}function _w(t){return new ne(n=>{let e=t[Ro]();if(le(e.subscribe))return e.subscribe(n);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function yw(t){return new ne(n=>{for(let e=0;e<t.length&&!n.closed;e++)n.next(t[e]);n.complete()})}function Sw(t){return new ne(n=>{t.then(e=>{n.closed||(n.next(e),n.complete())},e=>n.error(e)).then(null,cc)})}function Cw(t){return new ne(n=>{for(let e of t)if(n.next(e),n.closed)return;n.complete()})}function yb(t){return new ne(n=>{Ew(t,n).catch(e=>n.error(e))})}function Dw(t){return yb(xc(t))}function Ew(t,n){var e,i,r,o;return vb(this,void 0,void 0,function*(){try{for(e=_b(t);i=yield e.next(),!i.done;){let s=i.value;if(n.next(s),n.closed)return}}catch(s){r={error:s}}finally{try{i&&!i.done&&(o=e.return)&&(yield o.call(e))}finally{if(r)throw r.error}}n.complete()})}function Rt(t,n,e,i=0,r=!1){let o=n.schedule(function(){e(),r?t.add(this.schedule(null,i)):this.unsubscribe()},i);if(t.add(o),!r)return o}function Ic(t,n=0){return ue((e,i)=>{e.subscribe(fe(i,r=>Rt(i,t,()=>i.next(r),n),()=>Rt(i,t,()=>i.complete(),n),r=>Rt(i,t,()=>i.error(r),n)))})}function Mc(t,n=0){return ue((e,i)=>{i.add(t.schedule(()=>e.subscribe(i),n))})}function Sb(t,n){return ke(t).pipe(Mc(n),Ic(n))}function Cb(t,n){return ke(t).pipe(Mc(n),Ic(n))}function Db(t,n){return new ne(e=>{let i=0;return n.schedule(function(){i===t.length?e.complete():(e.next(t[i++]),e.closed||this.schedule())})})}function Eb(t,n){return new ne(e=>{let i;return Rt(e,n,()=>{i=t[Dc](),Rt(e,n,()=>{let r,o;try{({value:r,done:o}=i.next())}catch(s){e.error(s);return}o?e.complete():e.next(r)},0,!0)}),()=>le(i?.return)&&i.return()})}function Tc(t,n){if(!t)throw new Error("Iterable cannot be null");return new ne(e=>{Rt(e,n,()=>{let i=t[Symbol.asyncIterator]();Rt(e,n,()=>{i.next().then(r=>{r.done?e.complete():e.next(r.value)})},0,!0)})})}function xb(t,n){return Tc(xc(t),n)}function wb(t,n){if(t!=null){if(yc(t))return Sb(t,n);if(bc(t))return Db(t,n);if(_c(t))return Cb(t,n);if(Sc(t))return Tc(t,n);if(Ec(t))return Eb(t,n);if(wc(t))return xb(t,n)}throw Cc(t)}function Ve(t,n){return n?wb(t,n):ke(t)}function Y(...t){let n=On(t);return Ve(t,n)}function ta(t,n){let e=le(t)?t:()=>t,i=r=>r.error(e());return new ne(n?r=>n.schedule(i,0,r):i)}function na(t){return!!t&&(t instanceof ne||le(t.lift)&&le(t.subscribe))}var xr=No(t=>function(){t(this),this.name="EmptyError",this.message="no elements in sequence"});function Ib(t){return t instanceof Date&&!isNaN(t)}function oe(t,n){return ue((e,i)=>{let r=0;e.subscribe(fe(i,o=>{i.next(t.call(n,o,r++))}))})}var{isArray:xw}=Array;function ww(t,n){return xw(n)?t(...n):t(n)}function Nc(t){return oe(n=>ww(t,n))}var{isArray:Iw}=Array,{getPrototypeOf:Mw,prototype:Tw,keys:Nw}=Object;function kc(t){if(t.length===1){let n=t[0];if(Iw(n))return{args:n,keys:null};if(kw(n)){let e=Nw(n);return{args:e.map(i=>n[i]),keys:e}}}return{args:t,keys:null}}function kw(t){return t&&typeof t=="object"&&Mw(t)===Tw}function Ac(t,n){return t.reduce((e,i,r)=>(e[i]=n[r],e),{})}function ia(...t){let n=On(t),e=vc(t),{args:i,keys:r}=kc(t);if(i.length===0)return Ve([],n);let o=new ne(Aw(i,n,r?s=>Ac(r,s):Lt));return e?o.pipe(Nc(e)):o}function Aw(t,n,e=Lt){return i=>{Mb(n,()=>{let{length:r}=t,o=new Array(r),s=r,a=r;for(let l=0;l<r;l++)Mb(n,()=>{let c=Ve(t[l],n),d=!1;c.subscribe(fe(i,f=>{o[l]=f,d||(d=!0,a--),a||i.next(e(o.slice()))},()=>{--s||i.complete()}))},i)},i)}}function Mb(t,n,e){t?Rt(e,t,n):n()}function Tb(t,n,e,i,r,o,s,a){let l=[],c=0,d=0,f=!1,h=()=>{f&&!l.length&&!c&&n.complete()},p=E=>c<i?S(E):l.push(E),S=E=>{o&&n.next(E),c++;let $=!1;ke(e(E,d++)).subscribe(fe(n,H=>{r?.(H),o?p(H):n.next(H)},()=>{$=!0},void 0,()=>{if($)try{for(c--;l.length&&c<i;){let H=l.shift();s?Rt(n,s,()=>S(H)):S(H)}h()}catch(H){n.error(H)}}))};return t.subscribe(fe(n,p,()=>{f=!0,h()})),()=>{a?.()}}function Tt(t,n,e=1/0){return le(n)?Tt((i,r)=>oe((o,s)=>n(i,o,r,s))(ke(t(i,r))),e):(typeof n=="number"&&(e=n),ue((i,r)=>Tb(i,r,t,e)))}function Rc(t=1/0){return Tt(Lt,t)}function Nb(){return Rc(1)}function Ai(...t){return Nb()(Ve(t,On(t)))}function hn(t){return new ne(n=>{ke(t()).subscribe(n)})}function ra(...t){let n=vc(t),{args:e,keys:i}=kc(t),r=new ne(o=>{let{length:s}=e;if(!s){o.complete();return}let a=new Array(s),l=s,c=s;for(let d=0;d<s;d++){let f=!1;ke(e[d]).subscribe(fe(o,h=>{f||(f=!0,c--),a[d]=h},()=>l--,void 0,()=>{(!l||!f)&&(c||o.next(i?Ac(i,a):a),o.complete())}))}});return n?r.pipe(Nc(n)):r}function kb(t=0,n,e=mb){let i=-1;return n!=null&&(gc(n)?e=n:i=n),new ne(r=>{let o=Ib(t)?+t-e.now():t;o<0&&(o=0);let s=0;return e.schedule(function(){r.closed||(r.next(s++),0<=i?this.schedule(void 0,i):r.complete())},o)})}function Nt(...t){let n=On(t),e=pb(t,1/0),i=t;return i.length?i.length===1?ke(i[0]):Rc(e)(Ve(i,n)):We}function ge(t,n){return ue((e,i)=>{let r=0;e.subscribe(fe(i,o=>t.call(n,o,r++)&&i.next(o)))})}function Ab(t){return ue((n,e)=>{let i=!1,r=null,o=null,s=!1,a=()=>{if(o?.unsubscribe(),o=null,i){i=!1;let c=r;r=null,e.next(c)}s&&e.complete()},l=()=>{o=null,s&&e.complete()};n.subscribe(fe(e,c=>{i=!0,r=c,o||ke(t(c)).subscribe(o=fe(e,a,l))},()=>{s=!0,(!i||!o||o.closed)&&e.complete()}))})}function Oc(t,n=ea){return Ab(()=>kb(t,n))}function wr(t){return ue((n,e)=>{let i=null,r=!1,o;i=n.subscribe(fe(e,void 0,void 0,s=>{o=ke(t(s,wr(t)(n))),i?(i.unsubscribe(),i=null,o.subscribe(e)):r=!0})),r&&(i.unsubscribe(),i=null,o.subscribe(e))})}function Oo(t,n){return le(n)?Tt(t,n,1):Tt(t,1)}function oa(t,n=ea){return ue((e,i)=>{let r=null,o=null,s=null,a=()=>{if(r){r.unsubscribe(),r=null;let c=o;o=null,i.next(c)}};function l(){let c=s+t,d=n.now();if(d<c){r=this.schedule(void 0,c-d),i.add(r);return}a()}e.subscribe(fe(i,c=>{o=c,s=n.now(),r||(r=n.schedule(l,t),i.add(r))},()=>{a(),i.complete()},void 0,()=>{o=r=null}))})}function Rb(t){return ue((n,e)=>{let i=!1;n.subscribe(fe(e,r=>{i=!0,e.next(r)},()=>{i||e.next(t),e.complete()}))})}function Ae(t){return t<=0?()=>We:ue((n,e)=>{let i=0;n.subscribe(fe(e,r=>{++i<=t&&(e.next(r),t<=i&&e.complete())}))})}function Pc(t,n=Lt){return t=t??Rw,ue((e,i)=>{let r,o=!0;e.subscribe(fe(i,s=>{let a=n(s);(o||!t(r,a))&&(o=!1,r=a,i.next(s))}))})}function Rw(t,n){return t===n}function Ob(t=Ow){return ue((n,e)=>{let i=!1;n.subscribe(fe(e,r=>{i=!0,e.next(r)},()=>i?e.complete():e.error(t())))})}function Ow(){return new xr}function Ir(t){return ue((n,e)=>{try{n.subscribe(e)}finally{e.add(t)}})}function Kn(t,n){let e=arguments.length>=2;return i=>i.pipe(t?ge((r,o)=>t(r,o,i)):Lt,Ae(1),e?Rb(n):Ob(()=>new xr))}function Fc(t){return t<=0?()=>We:ue((n,e)=>{let i=[];n.subscribe(fe(e,r=>{i.push(r),t<i.length&&i.shift()},()=>{for(let r of i)e.next(r);e.complete()},void 0,()=>{i=null}))})}function Lc(){return ue((t,n)=>{let e,i=!1;t.subscribe(fe(n,r=>{let o=e;e=r,i&&n.next([o,r]),i=!0}))})}function sa(t={}){let{connector:n=()=>new x,resetOnError:e=!0,resetOnComplete:i=!0,resetOnRefCountZero:r=!0}=t;return o=>{let s,a,l,c=0,d=!1,f=!1,h=()=>{a?.unsubscribe(),a=void 0},p=()=>{h(),s=l=void 0,d=f=!1},S=()=>{let E=s;p(),E?.unsubscribe()};return ue((E,$)=>{c++,!f&&!d&&h();let H=l=l??n();$.add(()=>{c--,c===0&&!f&&!d&&(a=fh(S,r))}),H.subscribe($),!s&&c>0&&(s=new Xn({next:Se=>H.next(Se),error:Se=>{f=!0,h(),a=fh(p,e,Se),H.error(Se)},complete:()=>{d=!0,h(),a=fh(p,i),H.complete()}}),ke(E).subscribe(s))})(o)}}function fh(t,n,...e){if(n===!0){t();return}if(n===!1)return;let i=new Xn({next:()=>{i.unsubscribe(),t()}});return ke(n(...e)).subscribe(i)}function Vc(t,n,e){let i,r=!1;return t&&typeof t=="object"?{bufferSize:i=1/0,windowTime:n=1/0,refCount:r=!1,scheduler:e}=t:i=t??1/0,sa({connector:()=>new ki(i,n,e),resetOnError:!0,resetOnComplete:!1,resetOnRefCountZero:r})}function aa(t){return ge((n,e)=>t<=e)}function nt(...t){let n=On(t);return ue((e,i)=>{(n?Ai(t,e,n):Ai(t,e)).subscribe(i)})}function Ke(t,n){return ue((e,i)=>{let r=null,o=0,s=!1,a=()=>s&&!r&&i.complete();e.subscribe(fe(i,l=>{r?.unsubscribe();let c=0,d=o++;ke(t(l,d)).subscribe(r=fe(i,f=>i.next(n?n(l,f,d,c++):f),()=>{r=null,a()}))},()=>{s=!0,a()}))})}function Ce(t){return ue((n,e)=>{ke(t).subscribe(fe(e,()=>e.complete(),Ks)),!e.closed&&n.subscribe(e)})}function hh(t,n=!1){return ue((e,i)=>{let r=0;e.subscribe(fe(i,o=>{let s=t(o,r++);(s||n)&&i.next(o),!s&&i.complete()}))})}function dt(t,n,e){let i=le(t)||n||e?{next:t,error:n,complete:e}:t;return i?ue((r,o)=>{var s;(s=i.subscribe)===null||s===void 0||s.call(i);let a=!0;r.subscribe(fe(o,l=>{var c;(c=i.next)===null||c===void 0||c.call(i,l),o.next(l)},()=>{var l;a=!1,(l=i.complete)===null||l===void 0||l.call(i),o.complete()},l=>{var c;a=!1,(c=i.error)===null||c===void 0||c.call(i,l),o.error(l)},()=>{var l,c;a&&((l=i.unsubscribe)===null||l===void 0||l.call(i)),(c=i.finalize)===null||c===void 0||c.call(i)}))}):Lt}var Gc="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",w=class extends Error{code;constructor(n,e){super(ti(n,e)),this.code=n}};function Pw(t){return`NG0${Math.abs(t)}`}function ti(t,n){return`${Pw(t)}${n?": "+n:""}`}function xe(t){for(let n in t)if(t[n]===xe)return n;throw Error("")}function Bb(t,n){for(let e in n)Object.hasOwn(n,e)&&!Object.hasOwn(t,e)&&(t[e]=n[e])}function ha(t){if(typeof t=="string")return t;if(Array.isArray(t))return`[${t.map(ha).join(", ")}]`;if(t==null)return""+t;let n=t.overriddenName||t.name;if(n)return`${n}`;let e=t.toString();if(e==null)return""+e;let i=e.indexOf(`
`);return i>=0?e.slice(0,i):e}function Wc(t,n){return t?n?`${t} ${n}`:t:n||""}var Fw=xe({__forward_ref__:xe});function Pt(t){return t.__forward_ref__=Pt,t}function it(t){return wh(t)?t():t}function wh(t){return typeof t=="function"&&Object.hasOwn(t,Fw)&&t.__forward_ref__===Pt}function G(t){return{token:t.token,providedIn:t.providedIn||null,factory:t.factory,value:void 0}}function q(t){return{providers:t.providers||[],imports:t.imports||[]}}function ma(t){return Lw(t,qc)}function Ih(t){return ma(t)!==null}function Lw(t,n){return Object.hasOwn(t,n)&&t[n]||null}function Vw(t){let n=t?.[qc]??null;return n||null}function ph(t){return t&&Object.hasOwn(t,jc)?t[jc]:null}var qc=xe({\u0275prov:xe}),jc=xe({\u0275inj:xe}),b=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(n,e){this._desc=n,this.\u0275prov=void 0,typeof e=="number"?this.__NG_ELEMENT_ID__=e:e!==void 0&&(this.\u0275prov=G({token:this,providedIn:e.providedIn||"root",factory:e.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function Mh(t){return t&&!!t.\u0275providers}var pa=xe({\u0275cmp:xe}),ga=xe({\u0275dir:xe}),Th=xe({\u0275pipe:xe}),Nh=xe({\u0275mod:xe}),ca=xe({\u0275fac:xe}),Rr=xe({__NG_ELEMENT_ID__:xe}),Pb=xe({__NG_ENV_ID__:xe});function jb(t){return Zc(t,"@NgModule"),t[Nh]||null}function Ri(t){return Zc(t,"@Component"),t[pa]||null}function Yc(t){return Zc(t,"@Directive"),t[ga]||null}function Hb(t){return Zc(t,"@Pipe"),t[Th]||null}function Zc(t,n){if(t==null)throw new w(-919,!1)}function Xc(t){return typeof t=="string"?t:t==null?"":String(t)}var Ub=xe({ngErrorCode:xe}),Bw=xe({ngErrorMessage:xe}),jw=xe({ngTokenPath:xe});function kh(t,n){return zb("",-200,n)}function Kc(t,n){throw new w(-201,!1)}function zb(t,n,e){let i=new w(n,t);return i[Ub]=n,i[Bw]=t,e&&(i[jw]=e),i}function Hw(t){return t[Ub]}var gh;function $b(){return gh}function Vt(t){let n=gh;return gh=t,n}function Ah(t,n,e){let i=ma(t);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(e&8)return null;if(n!==void 0)return n;Kc(t,"")}var Lo=globalThis;var Uw={},Mr=Uw,zw="__NG_DI_FLAG__",vh=class{injector;constructor(n){this.injector=n}retrieve(n,e){let i=Tr(e)||0;try{return this.injector.get(n,i&8?null:Mr,i)}catch(r){if(To(r))return r;throw r}}};function $w(t,n=0){let e=sc();if(e===void 0)throw new w(-203,!1);if(e===null)return Ah(t,void 0,n);{let i=Gw(n),r=e.retrieve(t,i);if(To(r)){if(i.optional)return null;throw r}return r}}function A(t,n=0){return($b()||$w)(it(t),n)}function u(t,n){return A(t,Tr(n))}function Tr(t){return typeof t>"u"||typeof t=="number"?t:0|(t.optional&&8)|(t.host&&1)|(t.self&&2)|(t.skipSelf&&4)}function Gw(t){return{optional:!!(t&8),host:!!(t&1),self:!!(t&2),skipSelf:!!(t&4)}}function bh(t){let n=[];for(let e=0;e<t.length;e++){let i=it(t[e]);if(Array.isArray(i)){if(i.length===0)throw new w(900,!1);let r,o=0;for(let s=0;s<i.length;s++){let a=i[s],l=Ww(a);typeof l=="number"?l===-1?r=a.token:o|=l:r=a}n.push(A(r,o))}else n.push(A(i))}return n}function Ww(t){return t[zw]}function Nr(t,n){let e=Object.hasOwn(t,ca);return e?t[ca]:null}function Gb(t,n,e){if(t.length!==n.length)return!1;for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(e&&(r=e(r),o=e(o)),o!==r)return!1}return!0}function Wb(t){return t.flat(Number.POSITIVE_INFINITY)}function Qc(t,n){t.forEach(e=>Array.isArray(e)?Qc(e,n):n(e))}function Rh(t,n,e){n>=t.length?t.push(e):t.splice(n,0,e)}function va(t,n){return n>=t.length-1?t.pop():t.splice(n,1)[0]}function qb(t,n){let e=[];for(let i=0;i<t;i++)e.push(n);return e}function Yb(t,n,e,i){let r=t.length;if(r==n)t.push(e,i);else if(r===1)t.push(i,t[0]),t[0]=e;else{for(r--,t.push(t[r-1],t[r]);r>n;){let o=r-2;t[r]=t[o],r--}t[n]=e,t[n+1]=i}}function Jc(t,n,e){let i=Vo(t,n);return i>=0?t[i|1]=e:(i=~i,Yb(t,i,n,e)),i}function ed(t,n){let e=Vo(t,n);if(e>=0)return t[e|1]}function Vo(t,n){return qw(t,n,1)}function qw(t,n,e){let i=0,r=t.length>>e;for(;r!==i;){let o=i+(r-i>>1),s=t[o<<e];if(n===s)return o<<e;s>n?r=o:i=o+1}return~(r<<e)}var Oi={},St=[],Or=new b(""),ba=new b("",-1),Oh=new b(""),Fo=class{get(n,e=Mr){if(e===Mr){let r=zb("",-201);throw r.name="\u0275NotFound",r}return e}};function Fn(t){return{\u0275providers:t}}function Zb(t){return Fn([{provide:Or,multi:!0,useValue:t}])}function Xb(...t){return{\u0275providers:Ph(!0,t),\u0275fromNgModule:!0}}function Ph(t,...n){let e=[],i=new Set,r,o=s=>{e.push(s)};return Qc(n,s=>{let a=s;Hc(a,o,[],i)&&(r||=[],r.push(a))}),r!==void 0&&Kb(r,o),e}function Kb(t,n){for(let e=0;e<t.length;e++){let{ngModule:i,providers:r}=t[e];Fh(r,o=>{n(o,i)})}}function Hc(t,n,e,i){if(t=it(t),!t)return!1;let r=null,o=ph(t),s=!o&&Ri(t);if(!o&&!s){let l=t.ngModule;if(o=ph(l),o)r=l;else return!1}else{if(s&&!s.standalone)return!1;r=t}let a=i.has(r);if(s){if(a)return!1;if(i.add(r),s.dependencies){let l=typeof s.dependencies=="function"?s.dependencies():s.dependencies;for(let c of l)Hc(c,n,e,i)}}else if(o){if(o.imports!=null&&!a){i.add(r);let c;Qc(o.imports,d=>{Hc(d,n,e,i)&&(c||=[],c.push(d))}),c!==void 0&&Kb(c,n)}if(!a){let c=Nr(r)||(()=>new r);n({provide:r,useFactory:c,deps:St},r),n({provide:Oh,useValue:r,multi:!0},r),n({provide:Or,useValue:()=>A(r),multi:!0},r)}let l=o.providers;if(l!=null&&!a){let c=t;Fh(l,d=>{n(d,c)})}}else return!1;return r!==t&&t.providers!==void 0}function Fh(t,n){for(let e of t)Mh(e)&&(e=e.\u0275providers),Array.isArray(e)?Fh(e,n):n(e)}var Yw=xe({provide:String,useValue:xe});function Qb(t){return t!==null&&typeof t=="object"&&Yw in t}function Zw(t){return!!(t&&t.useExisting)}function Xw(t){return!!(t&&t.useFactory)}function kr(t){return typeof t=="function"}function Jb(t){return!!t.useClass}var _a=new b(""),Bc={},Fb={},mh;function Bo(){return mh===void 0&&(mh=new Fo),mh}var Me=class{},Ar=class extends Me{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(n,e,i,r){super(),this.parent=e,this.source=i,this.scopes=r,yh(n,s=>this.processProvider(s)),this.records.set(ba,Po(void 0,this)),r.has("environment")&&this.records.set(Me,Po(void 0,this));let o=this.records.get(_a);o!=null&&typeof o.value=="string"&&this.scopes.add(o.value),this.injectorDefTypes=new Set(this.get(Oh,St,{self:!0}))}retrieve(n,e){let i=Tr(e)||0;try{return this.get(n,Mr,i)}catch(r){if(To(r))return r;throw r}}destroy(){la(this),this._destroyed=!0;let n=Q(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let e=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of e)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),Q(n)}}onDestroy(n){return la(this),this._onDestroyHooks.push(n),()=>this.removeOnDestroy(n)}runInContext(n){la(this);let e=Rn(this),i=Vt(void 0),r;try{return n()}finally{Rn(e),Vt(i)}}get(n,e=Mr,i){if(la(this),Object.hasOwn(n,Pb))return n[Pb](this);let r=Tr(i),o,s=Rn(this),a=Vt(void 0);try{if(!(r&4)){let c=this.records.get(n);if(c===void 0){let d=tI(n)&&ma(n);d&&this.injectableDefInScope(d)?c=Po(_h(n),Bc):c=null,this.records.set(n,c)}if(c!=null)return this.hydrate(n,c,r)}let l=r&2?Bo():this.parent;return e=r&8&&e===Mr?null:e,l.get(n,e)}catch(l){let c=Hw(l);throw c===-200||c===-201?new w(c,null):l}finally{Vt(a),Rn(s)}}resolveInjectorInitializers(){let n=Q(null),e=Rn(this),i=Vt(void 0),r;try{let o=this.get(Or,St,{self:!0});for(let s of o)s()}finally{Rn(e),Vt(i),Q(n)}}toString(){return"R3Injector[...]"}processProvider(n){n=it(n);let e=kr(n)?n:it(n&&n.provide),i=Qw(n);if(!kr(n)&&n.multi===!0){let r=this.records.get(e);r||(r=Po(void 0,Bc,!0),r.factory=()=>bh(r.multi),this.records.set(e,r)),e=n,r.multi.push(n)}this.records.set(e,i)}hydrate(n,e,i){let r=Q(null);try{if(e.value===Fb)throw kh("");return e.value===Bc&&(e.value=Fb,e.value=e.factory(void 0,i)),typeof e.value=="object"&&e.value&&eI(e.value)&&this._ngOnDestroyHooks.add(e.value),e.value}finally{Q(r)}}injectableDefInScope(n){if(!n.providedIn)return!1;let e=it(n.providedIn);return typeof e=="string"?e==="any"||this.scopes.has(e):this.injectorDefTypes.has(e)}removeOnDestroy(n){let e=this._onDestroyHooks.indexOf(n);e!==-1&&this._onDestroyHooks.splice(e,1)}};function _h(t){let n=ma(t),e=n!==null?n.factory:Nr(t);if(e!==null)return e;if(t instanceof b)throw new w(-204,!1);if(t instanceof Function)return Kw(t);throw new w(-204,!1)}function Kw(t){if(t.length>0)throw new w(-204,!1);let e=Vw(t);return e!==null?()=>e.factory(t):()=>new t}function Qw(t){if(Qb(t))return Po(void 0,t.useValue);{let n=Lh(t);return Po(n,Bc)}}function Lh(t,n,e){let i;if(kr(t)){let r=it(t);return Nr(r)||_h(r)}else if(Qb(t))i=()=>it(t.useValue);else if(Xw(t))i=()=>t.useFactory(...bh(t.deps||[]));else if(Zw(t))i=(r,o)=>A(it(t.useExisting),o!==void 0&&o&8?8:void 0);else{let r=it(t&&(t.useClass||t.provide));if(Jw(t))i=()=>new r(...bh(t.deps));else return Nr(r)||_h(r)}return i}function la(t){if(t.destroyed)throw new w(-205,!1)}function Po(t,n,e=!1){return{factory:t,value:n,multi:e?[]:void 0}}function Jw(t){return!!t.deps}function eI(t){return t!==null&&typeof t=="object"&&typeof t.ngOnDestroy=="function"}function tI(t){return typeof t=="function"||typeof t=="object"&&t.ngMetadataName==="InjectionToken"}function yh(t,n){for(let e of t)Array.isArray(e)?yh(e,n):e&&Mh(e)?yh(e.\u0275providers,n):n(e)}function ot(t,n){let e;t instanceof Ar?(la(t),e=t):e=new vh(t);let i,r=Rn(e),o=Vt(void 0);try{return n()}finally{Rn(r),Vt(o)}}function e_(){return $b()!==void 0||sc()!=null}var mn=0,Z=1,te=2,rt=3,qt=4,pt=5,Pr=6,jo=7,Ye=8,Ln=9,pn=10,Fe=11,Ho=12,Vh=13,Pi=14,kt=15,Fi=16,Fr=17,Vn=18,Bn=19,Bh=20,Qn=21,td=22,Jn=23,Bt=24,Lr=25,jn=26,Ze=27,t_=1,jh=6,Vr=7,ya=8,Br=9,je=10;function ni(t){return Array.isArray(t)&&typeof t[t_]=="object"}function Yt(t){return Array.isArray(t)&&t[t_]===!0}function Hh(t){return(t.flags&4)!==0}function ii(t){return t.componentOffset>-1}function Sa(t){return(t.flags&1)===1}function gn(t){return!!t.template}function Uo(t){return(t[te]&512)!==0}function jr(t){return(t[te]&256)===256}var st=(function(t){return t[t.NONE=0]="NONE",t[t.HTML=1]="HTML",t[t.STYLE=2]="STYLE",t[t.SCRIPT=3]="SCRIPT",t[t.URL=4]="URL",t[t.RESOURCE_URL=5]="RESOURCE_URL",t[t.ATTRIBUTE_NO_BINDING=6]="ATTRIBUTE_NO_BINDING",t})(st||{});var Uh="svg",n_="math";function gt(t){for(;Array.isArray(t);)t=t[mn];return t}function zh(t,n){return gt(n[t])}function vn(t,n){return gt(n[t.index])}function nd(t,n){return t.data[n]}function i_(t,n){return t[n]}function Zt(t,n){let e=n[t];return ni(e)?e:e[mn]}function r_(t){return(t[te]&4)===4}function id(t){return(t[te]&128)===128}function o_(t){return Yt(t[rt])}function jt(t,n){return n==null?null:t[n]}function $h(t){t[Fr]=0}function Gh(t){t[te]&1024||(t[te]|=1024,id(t)&&Hr(t))}function s_(t,n){for(;t>0;)n=n[Pi],t--;return n}function Ca(t){return!!(t[te]&9216||t[Bt]?.dirty)}function rd(t){t[pn].changeDetectionScheduler?.notify(8),t[te]&64&&(t[te]|=1024),Ca(t)&&Hr(t)}function Hr(t){t[pn].changeDetectionScheduler?.notify(0);let n=ei(t);for(;n!==null&&!(n[te]&8192||(n[te]|=8192,!id(n)));)n=ei(n)}function od(t,n){if(jr(t))throw new w(911,!1);t[Qn]===null&&(t[Qn]=[]),t[Qn].push(n)}function a_(t,n){if(t[Qn]===null)return;let e=t[Qn].indexOf(n);e!==-1&&t[Qn].splice(e,1)}function ei(t){let n=t[rt];return Yt(n)?n[rt]:n}function Wh(t){return t[jo]??=[]}function qh(t){return t.cleanup??=[]}function l_(t,n,e,i){let r=Wh(n);r.push(e),t.firstCreatePass&&qh(t).push(i,r.length-1)}var he={lFrame:y_(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var Sh=!1;function c_(){return he.lFrame.elementDepthCount}function d_(){he.lFrame.elementDepthCount++}function Yh(){he.lFrame.elementDepthCount--}function Zh(){return he.bindingsEnabled}function Xh(){return he.skipHydrationRootTNode!==null}function Kh(t){return he.skipHydrationRootTNode===t}function Qh(){he.skipHydrationRootTNode=null}function se(){return he.lFrame.lView}function He(){return he.lFrame.tView}function J(t){return he.lFrame.contextLView=t,t[Ye]}function ee(t){return he.lFrame.contextLView=null,t}function vt(){let t=Jh();for(;t!==null&&t.type===64;)t=t.parent;return t}function Jh(){return he.lFrame.currentTNode}function u_(){let t=he.lFrame,n=t.currentTNode;return t.isParent?n:n.parent}function zo(t,n){let e=he.lFrame;e.currentTNode=t,e.isParent=n}function em(){return he.lFrame.isParent}function tm(){he.lFrame.isParent=!1}function f_(){return he.lFrame.contextLView}function nm(){return Sh}function da(t){let n=Sh;return Sh=t,n}function h_(){let t=he.lFrame,n=t.bindingRootIndex;return n===-1&&(n=t.bindingRootIndex=t.tView.bindingStartIndex),n}function m_(t){return he.lFrame.bindingIndex=t}function Ur(){return he.lFrame.bindingIndex++}function im(t){let n=he.lFrame,e=n.bindingIndex;return n.bindingIndex=n.bindingIndex+t,e}function p_(){return he.lFrame.inI18n}function g_(t,n){let e=he.lFrame;e.bindingIndex=e.bindingRootIndex=t,sd(n)}function v_(){return he.lFrame.currentDirectiveIndex}function sd(t){he.lFrame.currentDirectiveIndex=t}function b_(t){let n=he.lFrame.currentDirectiveIndex;return n===-1?null:t[n]}function ad(){return he.lFrame.currentQueryIndex}function Da(t){he.lFrame.currentQueryIndex=t}function nI(t){let n=t[Z];return n.type===2?n.declTNode:n.type===1?t[pt]:null}function rm(t,n,e){if(e&4){let r=n,o=t;for(;r=r.parent,r===null&&!(e&1);)if(r=nI(o),r===null||(o=o[Pi],r.type&10))break;if(r===null)return!1;n=r,t=o}let i=he.lFrame=__();return i.currentTNode=n,i.lView=t,!0}function ld(t){let n=__(),e=t[Z];he.lFrame=n,n.currentTNode=e.firstChild,n.lView=t,n.tView=e,n.contextLView=t,n.bindingIndex=e.bindingStartIndex,n.inI18n=!1}function __(){let t=he.lFrame,n=t===null?null:t.child;return n===null?y_(t):n}function y_(t){let n={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:t,child:null,inI18n:!1};return t!==null&&(t.child=n),n}function S_(){let t=he.lFrame;return he.lFrame=t.parent,t.currentTNode=null,t.lView=null,t}var om=S_;function cd(){let t=S_();t.isParent=!0,t.tView=null,t.selectedIndex=-1,t.contextLView=null,t.elementDepthCount=0,t.currentDirectiveIndex=-1,t.currentNamespace=null,t.bindingRootIndex=-1,t.bindingIndex=-1,t.currentQueryIndex=0}function C_(t){return(he.lFrame.contextLView=s_(t,he.lFrame.contextLView))[Ye]}function ri(){return he.lFrame.selectedIndex}function Li(t){he.lFrame.selectedIndex=t}function dd(){let t=he.lFrame;return nd(t.tView,t.selectedIndex)}function Vi(){he.lFrame.currentNamespace=Uh}function ud(){iI()}function iI(){he.lFrame.currentNamespace=null}function sm(){return he.lFrame.currentNamespace}var D_=!0;function fd(){return D_}function hd(t){D_=t}function Ch(t,n=null,e=null,i){let r=am(t,n,e,i);return r.resolveInjectorInitializers(),r}function am(t,n=null,e=null,i,r=new Set){let o=[e||St,Xb(t)],s;return new Ar(o,n||Bo(),s||null,r)}var R=class t{static THROW_IF_NOT_FOUND=Mr;static NULL=new Fo;static create(n,e){if(Array.isArray(n))return Ch({name:""},e,n,"");{let i=n.name??"";return Ch({name:i},n.parent,n.providers,i)}}static \u0275prov=G({token:t,providedIn:"any",factory:()=>A(ba)});static __NG_ELEMENT_ID__=-1},j=new b(""),qe=class{static __NG_ELEMENT_ID__=rI;static __NG_ENV_ID__=n=>n},Uc=class extends qe{_lView;constructor(n){super(),this._lView=n}get destroyed(){return jr(this._lView)}onDestroy(n){let e=this._lView;return od(e,n),()=>a_(e,n)}};function rI(){return new Uc(se())}var E_=!1,x_=new b(""),oi=(()=>{class t{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new tt(!1);debugTaskTracker=u(x_,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new ne(e=>{e.next(!1),e.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let e=this.taskId++;return this.pendingTasks.add(e),this.debugTaskTracker?.add(e),e}has(e){return this.pendingTasks.has(e)}remove(e){this.pendingTasks.delete(e),this.debugTaskTracker?.remove(e),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=G({token:t,providedIn:"root",factory:()=>new t})}return t})(),Dh=class extends x{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(n=!1){super(),this.__isAsync=n,e_()&&(this.destroyRef=u(qe,{optional:!0})??void 0,this.pendingTasks=u(oi,{optional:!0})??void 0)}emit(n){let e=Q(null);try{super.next(n)}finally{Q(e)}}subscribe(n,e,i){let r=n,o=e||(()=>null),s=i;if(n&&typeof n=="object"){let l=n;r=l.next?.bind(l),o=l.error?.bind(l),s=l.complete?.bind(l)}this.__isAsync&&(o=this.wrapInTimeout(o),r&&(r=this.wrapInTimeout(r)),s&&(s=this.wrapInTimeout(s)));let a=super.subscribe({next:r,error:o,complete:s});return n instanceof pe&&n.add(a),a}wrapInTimeout(n){return e=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{n(e)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},W=Dh;function zc(...t){}function lm(t){let n,e;function i(){t=zc;try{e!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(e),n!==void 0&&clearTimeout(n)}catch{}}return n=setTimeout(()=>{t(),i()}),typeof requestAnimationFrame=="function"&&(e=requestAnimationFrame(()=>{t(),i()})),()=>i()}function w_(t){return queueMicrotask(()=>t()),()=>{t=zc}}var cm="isAngularZone",ua=cm+"_ID",oI=0,k=class t{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new W(!1);onMicrotaskEmpty=new W(!1);onStable=new W(!1);onError=new W(!1);constructor(n){let{enableLongStackTrace:e=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:o=E_}=n;if(typeof Zone>"u")throw new w(908,!1);Zone.assertZonePatched();let s=this;s._nesting=0,s._outer=s._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(s._inner=s._inner.fork(new Zone.TaskTrackingZoneSpec)),e&&Zone.longStackTraceZoneSpec&&(s._inner=s._inner.fork(Zone.longStackTraceZoneSpec)),s.shouldCoalesceEventChangeDetection=!r&&i,s.shouldCoalesceRunChangeDetection=r,s.callbackScheduled=!1,s.scheduleInRootZone=o,lI(s)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(cm)===!0}static assertInAngularZone(){if(!t.isInAngularZone())throw new w(909,!1)}static assertNotInAngularZone(){if(t.isInAngularZone())throw new w(909,!1)}run(n,e,i){return this._inner.run(n,e,i)}runTask(n,e,i,r){let o=this._inner,s=o.scheduleEventTask("NgZoneEvent: "+r,n,sI,zc,zc);try{return o.runTask(s,e,i)}finally{o.cancelTask(s)}}runGuarded(n,e,i){return this._inner.runGuarded(n,e,i)}runOutsideAngular(n){return this._outer.run(n)}},sI={};function dm(t){if(t._nesting==0&&!t.hasPendingMicrotasks&&!t.isStable)try{t._nesting++,t.onMicrotaskEmpty.emit(null)}finally{if(t._nesting--,!t.hasPendingMicrotasks)try{t.runOutsideAngular(()=>t.onStable.emit(null))}finally{t.isStable=!0}}}function aI(t){if(t.isCheckStableRunning||t.callbackScheduled)return;t.callbackScheduled=!0;function n(){lm(()=>{t.callbackScheduled=!1,Eh(t),t.isCheckStableRunning=!0,dm(t),t.isCheckStableRunning=!1})}t.scheduleInRootZone?Zone.root.run(()=>{n()}):t._outer.run(()=>{n()}),Eh(t)}function lI(t){let n=()=>{aI(t)},e=oI++;t._inner=t._inner.fork({name:"angular",properties:{[cm]:!0,[ua]:e,[ua+e]:!0},onInvokeTask:(i,r,o,s,a,l)=>{if(cI(l))return i.invokeTask(o,s,a,l);try{return Lb(t),i.invokeTask(o,s,a,l)}finally{(t.shouldCoalesceEventChangeDetection&&s.type==="eventTask"||t.shouldCoalesceRunChangeDetection)&&n(),Vb(t)}},onInvoke:(i,r,o,s,a,l,c)=>{try{return Lb(t),i.invoke(o,s,a,l,c)}finally{t.shouldCoalesceRunChangeDetection&&!t.callbackScheduled&&!dI(l)&&n(),Vb(t)}},onHasTask:(i,r,o,s)=>{i.hasTask(o,s),r===o&&(s.change=="microTask"?(t._hasPendingMicrotasks=s.microTask,Eh(t),dm(t)):s.change=="macroTask"&&(t.hasPendingMacrotasks=s.macroTask))},onHandleError:(i,r,o,s)=>(i.handleError(o,s),t.runOutsideAngular(()=>t.onError.emit(s)),!1)})}function Eh(t){t._hasPendingMicrotasks||(t.shouldCoalesceEventChangeDetection||t.shouldCoalesceRunChangeDetection)&&t.callbackScheduled===!0?t.hasPendingMicrotasks=!0:t.hasPendingMicrotasks=!1}function Lb(t){t._nesting++,t.isStable&&(t.isStable=!1,t.onUnstable.emit(null))}function Vb(t){t._nesting--,dm(t)}var fa=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new W;onMicrotaskEmpty=new W;onStable=new W;onError=new W;run(n,e,i){return n.apply(e,i)}runGuarded(n,e,i){return n.apply(e,i)}runOutsideAngular(n){return n()}runTask(n,e,i,r){return n.apply(e,i)}};function cI(t){return I_(t,"__ignore_ng_zone__")}function dI(t){return I_(t,"__scheduler_tick__")}function I_(t,n){return!Array.isArray(t)||t.length!==1?!1:t[0]?.data?.[n]===!0}var Ot=class{_console=console;handleError(n){this._console.error("ERROR",n)}},Xt=new b("",{factory:()=>{let t=u(k),n=u(Me),e;return i=>{t.runOutsideAngular(()=>{n.destroyed&&!e?setTimeout(()=>{throw i}):(e??=n.get(Ot),e.handleError(i))})}}}),M_={provide:Or,useValue:()=>{let t=u(Ot,{optional:!0})},multi:!0},uI=new b("",{factory:()=>{let t=u(j).defaultView;if(!t)return;let n=u(Xt),e=o=>{n(o.reason),o.preventDefault()},i=o=>{o.error?n(o.error):n(new Error(o.message,{cause:o})),o.preventDefault()},r=()=>{t.addEventListener("unhandledrejection",e),t.addEventListener("error",i)};typeof Zone<"u"?Zone.root.run(r):r(),u(qe).onDestroy(()=>{t.removeEventListener("error",i),t.removeEventListener("unhandledrejection",e)})}});function um(){return Fn([Zb(()=>{u(uI)})])}function U(t,n){let[e,i,r]=Qf(t,n?.equal),o=e,s=o[et];return o.set=i,o.update=r,o.asReadonly=T_.bind(o),o}function T_(){let t=this[et];if(t.readonlyFn===void 0){let n=()=>this();n[et]=t,t.readonlyFn=n}return t.readonlyFn}var si=new b("",{factory:()=>fI}),fI="ng";var md=new b(""),zr=new b("",{providedIn:"platform",factory:()=>"unknown"}),Ea=new b(""),Bi=new b("",{factory:()=>u(j).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var $o=(()=>{class t{view;node;constructor(e,i){this.view=e,this.node=i}static __NG_ELEMENT_ID__=hI}return t})();function hI(){return new $o(se(),vt())}var Pn=class{},xa=new b("",{factory:()=>!0});var fm=new b(""),pd=(()=>{class t{static \u0275prov=G({token:t,providedIn:"root",factory:()=>new xh})}return t})(),xh=class{dirtyEffectCount=0;queues=new Map;add(n){this.enqueue(n),this.schedule(n)}schedule(n){n.dirty&&this.dirtyEffectCount++}remove(n){let e=n.zone,i=this.queues.get(e);i.has(n)&&(i.delete(n),n.dirty&&this.dirtyEffectCount--)}enqueue(n){let e=n.zone;this.queues.has(e)||this.queues.set(e,new Set);let i=this.queues.get(e);i.has(n)||i.add(n)}flush(){for(;this.dirtyEffectCount>0;){let n=!1;for(let[e,i]of this.queues)e===null?n||=this.flushQueue(i):n||=e.run(()=>this.flushQueue(i));n||(this.dirtyEffectCount=0)}}flushQueue(n){let e=!1;for(let i of n)i.dirty&&(this.dirtyEffectCount--,e=!0,i.run());return e}},$c=class{[et];constructor(n){this[et]=n}destroy(){this[et].destroy()}};function ut(t,n){let e=n?.injector??u(R),i=n?.manualCleanup!==!0?e.get(qe):null,r,o=e.get($o,null,{optional:!0}),s=e.get(Pn);return o!==null?(r=k_(o.view,s,t),i instanceof Uc&&i._lView===o.view&&(i=null)):r=gI(t,e.get(pd),s),r.injector=e,i!==null&&(r.onDestroyFns=[i.onDestroy(()=>r.destroy())]),new $c(r)}var N_=N(v({},eh),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let t=da(!1);try{th(this)}finally{da(t)}},cleanup(){if(!this.cleanupFns?.length)return;let t=Q(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],Q(t)}}}),mI=N(v({},N_),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(Ni(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.scheduler.remove(this)}}),pI=N(v({},N_),{consumerMarkedDirty(){this.view[te]|=8192,Hr(this.view),this.notifier.notify(13)},destroy(){if(Ni(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.view[Jn]?.delete(this)}});function k_(t,n,e){let i=Object.create(pI);return i.view=t,i.zone=typeof Zone<"u"?Zone.current:null,i.notifier=n,i.fn=A_(i,e),t[Jn]??=new Set,t[Jn].add(i),i.consumerMarkedDirty(i),i}function gI(t,n,e){let i=Object.create(mI);return i.fn=A_(i,t),i.scheduler=n,i.notifier=e,i.zone=typeof Zone<"u"?Zone.current:null,i.scheduler.add(i),i.notifier.notify(12),i}function A_(t,n){return()=>{n(e=>(t.cleanupFns??=[]).push(e))}}function Kt(t){return typeof t=="function"&&t[et]!==void 0}var gd=(()=>{class t{internalPendingTasks=u(oi);scheduler=u(Pn);errorHandler=u(Xt);add(){let e=this.internalPendingTasks.add();return()=>{this.internalPendingTasks.has(e)&&(this.scheduler.notify(11),this.internalPendingTasks.remove(e))}}run(e){let i=this.add();try{e().catch(this.errorHandler).finally(i)}catch(r){this.errorHandler(r),i()}}static \u0275prov=G({token:t,providedIn:"root",factory:()=>new t})}return t})();var sp=Symbol("InputSignalNode#UNSET"),vy=N(v({},Mo),{transformFn:void 0,applyValueToInputSignal(t,n){yr(t,n)}});function Pa(t){return{toString:t}.toString()}var _e=(function(t){return t[t.TemplateCreateStart=0]="TemplateCreateStart",t[t.TemplateCreateEnd=1]="TemplateCreateEnd",t[t.TemplateUpdateStart=2]="TemplateUpdateStart",t[t.TemplateUpdateEnd=3]="TemplateUpdateEnd",t[t.LifecycleHookStart=4]="LifecycleHookStart",t[t.LifecycleHookEnd=5]="LifecycleHookEnd",t[t.OutputStart=6]="OutputStart",t[t.OutputEnd=7]="OutputEnd",t[t.BootstrapApplicationStart=8]="BootstrapApplicationStart",t[t.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",t[t.BootstrapComponentStart=10]="BootstrapComponentStart",t[t.BootstrapComponentEnd=11]="BootstrapComponentEnd",t[t.ChangeDetectionStart=12]="ChangeDetectionStart",t[t.ChangeDetectionEnd=13]="ChangeDetectionEnd",t[t.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",t[t.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",t[t.AfterRenderHooksStart=16]="AfterRenderHooksStart",t[t.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",t[t.ComponentStart=18]="ComponentStart",t[t.ComponentEnd=19]="ComponentEnd",t[t.DeferBlockStateStart=20]="DeferBlockStateStart",t[t.DeferBlockStateEnd=21]="DeferBlockStateEnd",t[t.DynamicComponentStart=22]="DynamicComponentStart",t[t.DynamicComponentEnd=23]="DynamicComponentEnd",t[t.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",t[t.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",t})(_e||{}),xd=class{previousValue;currentValue;firstChange;constructor(n,e,i){this.previousValue=n,this.currentValue=e,this.firstChange=i}isFirstChange(){return this.firstChange}};function by(t,n,e,i){n!==null?n.applyValueToInputSignal(n,i):t[e]=i}var _y=null,at=(()=>{_y=R_;let t=()=>R_;return t.ngInherit=!0,t})();function MI(){return _y}function R_(t){return t.type.prototype.ngOnChanges&&(t.setInput=NI),TI}function TI(){let t=yy(this),n=t?.current;if(n){let e=t.previous;if(e===Oi)t.previous=n;else for(let i in n)e[i]=n[i];t.current=null,this.ngOnChanges(n)}}function NI(t,n,e,i,r){let o=this.declaredInputs[i],s=yy(t)||kI(t,{previous:Oi,current:null}),a=s.current||(s.current={}),l=s.previous,c=l[o];a[o]=new xd(c&&c.currentValue,e,l===Oi),by(t,n,r,e)}var Dm="__ngSimpleChanges__";function yy(t){return Object.hasOwn(t,Dm)&&t[Dm]||null}function kI(t,n){return t[Dm]=n}var O_=[];var we=function(t,n=null,e){for(let i=0;i<O_.length;i++){let r=O_[i];r(t,n,e)}};function AI(t,n,e){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:o}=n.type.prototype;if(i){let s=MI()(n);(e.preOrderHooks??=[]).push(t,s),(e.preOrderCheckHooks??=[]).push(t,s)}r&&(e.preOrderHooks??=[]).push(0-t,r),o&&((e.preOrderHooks??=[]).push(t,o),(e.preOrderCheckHooks??=[]).push(t,o))}function Sy(t,n){for(let e=n.directiveStart,i=n.directiveEnd;e<i;e++){let o=t.data[e].type.prototype,{ngAfterContentInit:s,ngAfterContentChecked:a,ngAfterViewInit:l,ngAfterViewChecked:c,ngOnDestroy:d}=o;s&&(t.contentHooks??=[]).push(-e,s),a&&((t.contentHooks??=[]).push(e,a),(t.contentCheckHooks??=[]).push(e,a)),l&&(t.viewHooks??=[]).push(-e,l),c&&((t.viewHooks??=[]).push(e,c),(t.viewCheckHooks??=[]).push(e,c)),d!=null&&(t.destroyHooks??=[]).push(e,d)}}function yd(t,n,e){Cy(t,n,3,e)}function Sd(t,n,e,i){(t[te]&3)===e&&Cy(t,n,e,i)}function hm(t,n){let e=t[te];(e&3)===n&&(e&=16383,e+=1,t[te]=e)}function Cy(t,n,e,i){let r=i!==void 0?t[Fr]&65535:0,o=i??-1,s=n.length-1,a=0;for(let l=r;l<s;l++)if(typeof n[l+1]=="number"){if(a=n[l],i!=null&&a>=i)break}else n[l]<0&&(t[Fr]+=65536),(a<o||o==-1)&&(RI(t,e,n,l),t[Fr]=(t[Fr]&4294901760)+l+2),l++}function P_(t,n){we(_e.LifecycleHookStart,t,n);let e=Q(null);try{n.call(t)}finally{Q(e),we(_e.LifecycleHookEnd,t,n)}}function RI(t,n,e,i){let r=e[i]<0,o=e[i+1],s=r?-e[i]:e[i],a=t[s];r?t[te]>>14<t[Fr]>>16&&(t[te]&3)===n&&(t[te]+=16384,P_(a,o)):P_(a,o)}var Wo=-1,Gr=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(n,e,i,r){this.factory=n,this.name=r,this.canSeeViewProviders=e,this.injectImpl=i}};function OI(t){return(t.flags&8)!==0}function PI(t){return(t.flags&16)!==0}function FI(t,n,e){let i=0;for(;i<e.length;){let r=e[i];if(typeof r=="number"){if(r!==0)break;i++;let o=e[i++],s=e[i++],a=e[i++];t.setAttribute(n,s,a,o)}else{let o=r,s=e[++i];LI(o)?t.setProperty(n,o,s):t.setAttribute(n,o,s),i++}}return i}function Dy(t){return t===3||t===4||t===6}function LI(t){return t.charCodeAt(0)===64}function qo(t,n){if(!(n===null||n.length===0))if(t===null||t.length===0)t=n.slice();else{let e=-1;for(let i=0;i<n.length;i++){let r=n[i];typeof r=="number"?e=r:e===0||(e===-1||e===2?F_(t,e,r,null,n[++i]):F_(t,e,r,null,null))}}return t}function F_(t,n,e,i,r){let o=0,s=t.length;if(n===-1)s=-1;else for(;o<t.length;){let a=t[o++];if(typeof a=="number"){if(a===n){s=-1;break}else if(a>n){s=o-1;break}}}for(;o<t.length;){let a=t[o];if(typeof a=="number")break;if(a===e){r!==null&&(t[o+1]=r);return}o++,r!==null&&o++}s!==-1&&(t.splice(s,0,n),o=s+1),t.splice(o++,0,e),r!==null&&t.splice(o++,0,r)}function Ey(t){return t!==Wo}function wd(t){return t&32767}function VI(t){return t>>16}function Id(t,n){let e=VI(t),i=n;for(;e>0;)i=i[Pi],e--;return i}var Em=!0;function L_(t){let n=Em;return Em=t,n}var BI=256,xy=BI-1,wy=5,jI=0,Hn={};function HI(t,n,e){let i;typeof e=="string"?i=e.charCodeAt(0)||0:Object.hasOwn(e,Rr)&&(i=e[Rr]),i==null&&(i=e[Rr]=jI++);let r=i&xy,o=1<<r;n.data[t+(r>>wy)]|=o}function Md(t,n){let e=Iy(t,n);if(e!==-1)return e;let i=n[Z];i.firstCreatePass&&(t.injectorIndex=n.length,mm(i.data,t),mm(n,null),mm(i.blueprint,null));let r=ap(t,n),o=t.injectorIndex;if(Ey(r)){let s=wd(r),a=Id(r,n),l=a[Z].data;for(let c=0;c<8;c++)n[o+c]=a[s+c]|l[s+c]}return n[o+8]=r,o}function mm(t,n){t.push(0,0,0,0,0,0,0,0,n)}function Iy(t,n){return t.injectorIndex===-1||t.parent&&t.parent.injectorIndex===t.injectorIndex||n[t.injectorIndex+8]===null?-1:t.injectorIndex}function ap(t,n){if(t.parent&&t.parent.injectorIndex!==-1)return t.parent.injectorIndex;let e=0,i=null,r=n;for(;r!==null;){if(i=Ay(r),i===null)return Wo;if(e++,r=r[Pi],i.injectorIndex!==-1)return i.injectorIndex|e<<16}return Wo}function xm(t,n,e){HI(t,n,e)}function UI(t,n){if(n==="class")return t.classes;if(n==="style")return t.styles;let e=t.attrs;if(e){let i=e.length,r=0;for(;r<i;){let o=e[r];if(Dy(o))break;if(o===0)r=r+2;else if(typeof o=="number")for(r++;r<i&&typeof e[r]=="string";)r++;else{if(o===n)return e[r+1];r=r+2}}}return null}function My(t,n,e){if(e&8||t!==void 0)return t;Kc(n,"NodeInjector")}function Ty(t,n,e,i){if(e&8&&i===void 0&&(i=null),(e&3)===0){let r=t[Ln],o=Vt(void 0);try{return r?r.get(n,i,e&8):Ah(n,i,e&8)}finally{Vt(o)}}return My(i,n,e)}function Ny(t,n,e,i=0,r){if(t!==null){if(n[te]&2048&&!(i&2)){let s=WI(t,n,e,i,Hn);if(s!==Hn)return s}let o=ky(t,n,e,i,Hn);if(o!==Hn)return o}return Ty(n,e,i,r)}function ky(t,n,e,i,r){let o=$I(e);if(typeof o=="function"){if(!rm(n,t,i))return i&1?My(r,e,i):Ty(n,e,i,r);try{let s;if(s=o(i),s==null&&!(i&8))Kc(e);else return s}finally{om()}}else if(typeof o=="number"){let s=null,a=Iy(t,n),l=Wo,c=i&1?n[kt][pt]:null;for((a===-1||i&4)&&(l=a===-1?ap(t,n):n[a+8],l===Wo||!B_(i,!1)?a=-1:(s=n[Z],a=wd(l),n=Id(l,n)));a!==-1;){let d=n[Z];if(V_(o,a,d.data)){let f=zI(a,n,e,s,i,c);if(f!==Hn)return f}l=n[a+8],l!==Wo&&B_(i,n[Z].data[a+8]===c)&&V_(o,a,n)?(s=d,a=wd(l),n=Id(l,n)):a=-1}}return r}function zI(t,n,e,i,r,o){let s=n[Z],a=s.data[t+8],l=i==null?ii(a)&&Em:i!=s&&(a.type&3)!==0,c=r&1&&o===a,d=Cd(a,s,e,l,c);return d!==null?Ta(n,s,d,a,r):Hn}function Cd(t,n,e,i,r){let o=t.providerIndexes,s=n.data,a=o&1048575,l=t.directiveStart,c=t.directiveEnd,d=o>>20,f=i?a:a+d,h=r?a+d:c;for(let p=f;p<h;p++){let S=s[p];if(p<l&&e===S||p>=l&&S.type===e)return p}if(r){let p=s[l];if(p&&gn(p)&&p.type===e)return l}return null}function Ta(t,n,e,i,r){let o=t[e],s=n.data;if(o instanceof Gr){let a=o;if(a.resolving)throw kh("");let l=L_(a.canSeeViewProviders);a.resolving=!0;let c=s[e].type||s[e],d,f=a.injectImpl?Vt(a.injectImpl):null,h=rm(t,i,0);try{o=t[e]=a.factory(void 0,r,s,t,i),n.firstCreatePass&&e>=i.directiveStart&&AI(e,s[e],n)}finally{f!==null&&Vt(f),L_(l),a.resolving=!1,om()}}return o}function $I(t){if(typeof t=="string")return t.charCodeAt(0)||0;let n=Object.hasOwn(t,Rr)?t[Rr]:void 0;return typeof n=="number"?n>=0?n&xy:GI:n}function V_(t,n,e){let i=1<<t;return!!(e[n+(t>>wy)]&i)}function B_(t,n){return!(t&2)&&!(t&1&&n)}var ji=class{_tNode;_lView;constructor(n,e){this._tNode=n,this._lView=e}get(n,e,i){return Ny(this._tNode,this._lView,n,Tr(i),e)}};function GI(){return new ji(vt(),se())}function Ht(t){return Pa(()=>{let n=t.prototype.constructor,e=n[ca]||wm(n),i=Object.prototype,r=Object.getPrototypeOf(t.prototype).constructor;for(;r&&r!==i;){let o=r[ca]||wm(r);if(o&&o!==e)return o;r=Object.getPrototypeOf(r)}return o=>new o})}function wm(t){return wh(t)?()=>{let n=wm(it(t));return n&&n()}:Nr(t)}function WI(t,n,e,i,r){let o=t,s=n;for(;o!==null&&s!==null&&s[te]&2048&&!Uo(s);){let a=ky(o,s,e,i|2,Hn);if(a!==Hn)return a;i&=-5;let l=o.parent;if(!l){let c=s[Bh];if(c){let d=c.get(e,Hn,i);if(d!==Hn)return d}l=Ay(s),s=s[Pi]}o=l}return r}function Ay(t){let n=t[Z],e=n.type;return e===2?n.declTNode:e===1?t[pt]:null}function lp(t){return UI(vt(),t)}function I(t){return{token:t.token,providedIn:t.autoProvided===!1?null:"root",factory:t.factory,value:void 0}}function qI(){return Jo(vt(),se())}function Jo(t,n){return new O(vn(t,n))}var O=(()=>{class t{nativeElement;constructor(e){this.nativeElement=e}static __NG_ELEMENT_ID__=qI}return t})();function Ry(t){return t instanceof O?t.nativeElement:t}function YI(){return this._results[Symbol.iterator]()}var ai=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new x}constructor(n=!1){this._emitDistinctChangesOnly=n}get(n){return this._results[n]}map(n){return this._results.map(n)}filter(n){return this._results.filter(n)}find(n){return this._results.find(n)}reduce(n,e){return this._results.reduce(n,e)}forEach(n){this._results.forEach(n)}some(n){return this._results.some(n)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(n,e){this.dirty=!1;let i=Wb(n);(this._changesDetected=!Gb(this._results,i,e))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(n){this._onDirty=n}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=YI};function Oy(t){return(t.flags&128)===128}var cp=(function(t){return t[t.OnPush=0]="OnPush",t[t.Eager=1]="Eager",t[t.Default=1]="Default",t})(cp||{}),Py=new Map,ZI=0;function XI(){return ZI++}function KI(t){Py.set(t[Bn],t)}function Im(t){Py.delete(t[Bn])}var j_="__ngContext__";function Yo(t,n){ni(n)?(t[j_]=n[Bn],KI(n)):t[j_]=n}function Fy(t){return Vy(t[Ho])}function Ly(t){return Vy(t[qt])}function Vy(t){for(;t!==null&&!Yt(t);)t=t[qt];return t}var Mm;function dp(t){Mm=t}function By(){if(Mm!==void 0)return Mm;if(typeof document<"u")return document;throw new w(210,!1)}var jy="r";var Hy="di";var Uy=!1,zy=new b("",{factory:()=>Uy});var H_=new WeakMap;function QI(t,n){if(t==null||typeof t!="object")return;let e=H_.get(t);e||(e=new WeakSet,H_.set(t,e)),e.add(n)}var JI=(t,n,e,i)=>{};function eM(t,n,e,i){JI(t,n,e,i)}function Bd(t){return(t.flags&32)===32}var tM=()=>null;function $y(t,n,e=!1){return tM(t,n,e)}function Gy(t,n){let e=t.contentQueries;if(e!==null){let i=Q(null);try{for(let r=0;r<e.length;r+=2){let o=e[r],s=e[r+1];if(s!==-1){let a=t.data[s];Da(o),a.contentQueries(2,n[s],s)}}}finally{Q(i)}}}function Tm(t,n,e){Da(0);let i=Q(null);try{n(t,e)}finally{Q(i)}}function Wy(t,n,e){if(Hh(n)){let i=Q(null);try{let r=n.directiveStart,o=n.directiveEnd;for(let s=r;s<o;s++){let a=t.data[s];if(a.contentQueries){let l=e[s];a.contentQueries(1,l,s)}}}finally{Q(i)}}}var yn=(function(t){return t[t.Emulated=0]="Emulated",t[t.None=2]="None",t[t.ShadowDom=3]="ShadowDom",t[t.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",t})(yn||{});var vd;function nM(){if(vd===void 0&&(vd=null,Lo.trustedTypes))try{vd=Lo.trustedTypes.createPolicy("angular",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch{}return vd}function jd(t){return nM()?.createHTML(t)||t}var li=class{changingThisBreaksApplicationSecurity;constructor(n){this.changingThisBreaksApplicationSecurity=n}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${Gc})`}},Nm=class extends li{getTypeName(){return"HTML"}},km=class extends li{getTypeName(){return"Style"}},Am=class extends li{getTypeName(){return"Script"}},Rm=class extends li{getTypeName(){return"URL"}},Om=class extends li{getTypeName(){return"ResourceURL"}};function Sn(t){return t instanceof li?t.changingThisBreaksApplicationSecurity:t}function zi(t,n){let e=qy(t);if(e!=null&&e!==n){if(e==="ResourceURL"&&n==="URL")return!0;throw new Error(`Required a safe ${n}, got a ${e} (see ${Gc})`)}return e===n}function qy(t){return t instanceof li&&t.getTypeName()||null}function up(t){return new Nm(t)}function fp(t){return new km(t)}function hp(t){return new Am(t)}function mp(t){return new Rm(t)}function pp(t){return new Om(t)}function iM(t){let n=new Fm(t);return rM()?new Pm(n):n}var Pm=class{inertDocumentHelper;constructor(n){this.inertDocumentHelper=n}getInertBodyElement(n){n="<body><remove></remove>"+n;try{let e=new window.DOMParser().parseFromString(jd(n),"text/html").body;return e===null?this.inertDocumentHelper.getInertBodyElement(n):(e.firstChild?.remove(),e)}catch{return null}}},Fm=class{defaultDoc;inertDocument;constructor(n){this.defaultDoc=n,this.inertDocument=this.defaultDoc.implementation.createHTMLDocument("sanitization-inert")}getInertBodyElement(n){let e=this.inertDocument.createElement("template");return e.innerHTML=jd(n),e}};function rM(){try{return!!new window.DOMParser().parseFromString(jd(""),"text/html")}catch{return!1}}var oM=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function Fa(t){return t=String(t),t.match(oM)?t:"unsafe:"+t}function ci(t){let n=Object.create(null);for(let e of t.split(","))n[e]=!0;return n}function La(...t){let n=Object.create(null);for(let e of t)for(let i in e)Object.hasOwn(e,i)&&(n[i]=!0);return n}var Yy=ci("area,br,col,hr,img,wbr"),Zy=ci("colgroup,dd,dt,li,p,tbody,td,tfoot,th,thead,tr"),Xy=ci("rp,rt"),sM=La(Xy,Zy),aM=La(Zy,ci("address,article,aside,blockquote,caption,center,del,details,dialog,dir,div,dl,figure,figcaption,footer,h1,h2,h3,h4,h5,h6,header,hgroup,hr,ins,main,map,menu,nav,ol,pre,section,summary,table,ul")),lM=La(Xy,ci("a,abbr,acronym,audio,b,bdi,bdo,big,br,cite,code,del,dfn,em,font,i,img,ins,kbd,label,map,mark,picture,q,ruby,rp,rt,s,samp,small,source,span,strike,strong,sub,sup,time,track,tt,u,var,video")),U_=La(Yy,aM,lM,sM),Ky=ci("background,cite,href,itemtype,longdesc,poster,src,xlink:href"),cM=ci("abbr,accesskey,align,alt,autoplay,axis,bgcolor,border,cellpadding,cellspacing,class,clear,color,cols,colspan,compact,controls,coords,datetime,default,dir,download,face,headers,height,hidden,hreflang,hspace,ismap,itemscope,itemprop,kind,label,lang,language,loop,media,muted,nohref,nowrap,open,preload,rel,rev,role,rows,rowspan,rules,scope,scrolling,shape,size,sizes,span,srclang,srcset,start,summary,tabindex,target,title,translate,type,usemap,valign,value,vspace,width"),dM=ci("aria-activedescendant,aria-atomic,aria-autocomplete,aria-busy,aria-checked,aria-colcount,aria-colindex,aria-colspan,aria-controls,aria-current,aria-describedby,aria-details,aria-disabled,aria-dropeffect,aria-errormessage,aria-expanded,aria-flowto,aria-grabbed,aria-haspopup,aria-hidden,aria-invalid,aria-keyshortcuts,aria-label,aria-labelledby,aria-level,aria-live,aria-modal,aria-multiline,aria-multiselectable,aria-orientation,aria-owns,aria-placeholder,aria-posinset,aria-pressed,aria-readonly,aria-relevant,aria-required,aria-roledescription,aria-rowcount,aria-rowindex,aria-rowspan,aria-selected,aria-setsize,aria-sort,aria-valuemax,aria-valuemin,aria-valuenow,aria-valuetext"),uM=La(Ky,cM,dM),fM=ci("script,style,template"),Lm=class{sanitizedSomething=!1;buf=[];sanitizeChildren(n){let e=n.firstChild,i=!0,r=[];for(;e;){if(e.nodeType===Node.ELEMENT_NODE?i=this.startElement(e):e.nodeType===Node.TEXT_NODE?this.chars(e.nodeValue):this.sanitizedSomething=!0,i&&e.firstChild){r.push(e),e=pM(e);continue}for(;e;){e.nodeType===Node.ELEMENT_NODE&&this.endElement(e);let o=mM(e);if(o){e=o;break}e=r.pop()}}return this.buf.join("")}startElement(n){let e=z_(n).toLowerCase();if(!Object.hasOwn(U_,e))return this.sanitizedSomething=!0,!Object.hasOwn(fM,e);this.buf.push("<"),this.buf.push(e);let i=n.attributes;for(let r=0;r<i.length;r++){let o=i.item(r),s=o.name,a=s.toLowerCase();if(!Object.hasOwn(uM,a)){this.sanitizedSomething=!0;continue}let l=o.value;Ky[a]&&(l=Fa(l)),this.buf.push(" ",s,'="',$_(l),'"')}return this.buf.push(">"),!0}endElement(n){let e=z_(n).toLowerCase();Object.hasOwn(U_,e)&&!Object.hasOwn(Yy,e)&&(this.buf.push("</"),this.buf.push(e),this.buf.push(">"))}chars(n){this.buf.push($_(n))}};function hM(t,n){return(t.compareDocumentPosition(n)&Node.DOCUMENT_POSITION_CONTAINED_BY)!==Node.DOCUMENT_POSITION_CONTAINED_BY}function mM(t){let n=t.nextSibling;if(n&&t!==n.previousSibling)throw Qy(n);return n}function pM(t){let n=t.firstChild;if(n&&hM(t,n))throw Qy(n);return n}function z_(t){let n=t.nodeName;return typeof n=="string"?n:"FORM"}function Qy(t){return new Error(`Failed to sanitize html because the element is clobbered: ${t.outerHTML}`)}var gM=/[\uD800-\uDBFF][\uDC00-\uDFFF]/g,vM=/([^\#-~ |!])/g;function $_(t){return t.replace(/&/g,"&amp;").replace(gM,function(n){let e=n.charCodeAt(0),i=n.charCodeAt(1);return"&#"+((e-55296)*1024+(i-56320)+65536)+";"}).replace(vM,function(n){return"&#"+n.charCodeAt(0)+";"}).replace(/</g,"&lt;").replace(/>/g,"&gt;")}var bd;function gp(t,n){let e=null;try{bd=bd||iM(t);let i=n?String(n):"";e=bd.getInertBodyElement(i);let r=5,o=i;do{if(r===0)throw new Error("Failed to sanitize html because the input is unstable");r--,i=o,o=e.innerHTML,e=bd.getInertBodyElement(i)}while(i!==o);let a=new Lm().sanitizeChildren(G_(e)||e);return jd(a)}finally{if(e){let i=G_(e)||e;for(;i.firstChild;)i.firstChild.remove()}}}function G_(t){return"content"in t&&bM(t)?t.content:null}function bM(t){return t.nodeType===Node.ELEMENT_NODE&&t.nodeName==="TEMPLATE"}function _M(t,n){return t.createText(n)}function yM(t,n,e){t.setValue(n,e)}function Jy(t,n,e){return t.createElement(n,e)}function $r(t,n,e,i,r){t.insertBefore(n,e,i,r)}function e0(t,n,e){t.appendChild(n,e)}function W_(t,n,e,i,r){i!==null?$r(t,n,e,i,r):e0(t,n,e)}function t0(t,n,e,i){t.removeChild(null,n,e,i)}function SM(t,n,e){t.setAttribute(n,"style",e)}function CM(t,n,e){e===""?t.removeAttribute(n,"class"):t.setAttribute(n,"class",e)}function n0(t,n,e){let{mergedAttrs:i,classes:r,styles:o}=e;i!==null&&FI(t,n,i),r!==null&&CM(t,n,r),o!==null&&SM(t,n,o)}function qr(t){let n=DM();return n?n.sanitize(st.URL,t)||"":zi(t,"URL")?Sn(t):Fa(Xc(t))}function DM(){let t=se();return t&&t[pn].sanitizer}function vp(t){return t.ownerDocument.defaultView}function EM(t){return t instanceof Function?t():t}function xM(t,n,e){let i=t.length;for(;;){let r=t.indexOf(n,e);if(r===-1)return r;if(r===0||t.charCodeAt(r-1)<=32){let o=n.length;if(r+o===i||t.charCodeAt(r+o)<=32)return r}e=r+1}}var i0="ng-template";function wM(t,n,e,i){let r=0;if(i){for(;r<n.length&&typeof n[r]=="string";r+=2)if(n[r]==="class"&&xM(n[r+1].toLowerCase(),e,0)!==-1)return!0}else if(bp(t))return!1;if(r=n.indexOf(1,r),r>-1){let o;for(;++r<n.length&&typeof(o=n[r])=="string";)if(o.toLowerCase()===e)return!0}return!1}function bp(t){return t.type===4&&t.value!==i0}function IM(t,n,e){let i=t.type===4&&!e?i0:t.value;return n===i}function MM(t,n,e){let i=4,r=t.attrs,o=r!==null?kM(r):0,s=!1;for(let a=0;a<n.length;a++){let l=n[a];if(typeof l=="number"){if(!s&&!bn(i)&&!bn(l))return!1;if(s&&bn(l))continue;s=!1,i=l|i&1;continue}if(!s)if(i&4){if(i=2|i&1,l!==""&&!IM(t,l,e)||l===""&&n.length===1){if(bn(i))return!1;s=!0}}else if(i&8){if(r===null||!wM(t,r,l,e)){if(bn(i))return!1;s=!0}}else{let c=n[++a],d=TM(l,r,bp(t),e);if(d===-1){if(bn(i))return!1;s=!0;continue}if(c!==""){let f;if(d>o?f="":f=r[d+1].toLowerCase(),i&2&&c!==f){if(bn(i))return!1;s=!0}}}}return bn(i)||s}function bn(t){return(t&1)===0}function TM(t,n,e,i){if(n===null)return-1;let r=0;if(i||!e){let o=!1;for(;r<n.length;){let s=n[r];if(s===t)return r;if(s===3||s===6)o=!0;else if(s===1||s===2){let a=n[++r];for(;typeof a=="string";)a=n[++r];continue}else{if(s===4)break;if(s===0){r+=4;continue}}r+=o?1:2}return-1}else return AM(n,t)}function r0(t,n,e=!1){for(let i=0;i<n.length;i++)if(MM(t,n[i],e))return!0;return!1}function NM(t){let n=t.attrs;if(n!=null){let e=n.indexOf(5);if((e&1)===0)return n[e+1]}return null}function kM(t){for(let n=0;n<t.length;n++){let e=t[n];if(Dy(e))return n}return t.length}function AM(t,n){let e=t.indexOf(4);if(e>-1)for(e++;e<t.length;){let i=t[e];if(typeof i=="number")return-1;if(i===n)return e;e++}return-1}function RM(t,n){e:for(let e=0;e<n.length;e++){let i=n[e];if(t.length===i.length){for(let r=0;r<t.length;r++)if(t[r]!==i[r])continue e;return!0}}return!1}function q_(t,n){return t?":not("+n.trim()+")":n}function OM(t){let n=t[0],e=1,i=2,r="",o=!1;for(;e<t.length;){let s=t[e];if(typeof s=="string")if(i&2){let a=t[++e];r+="["+s+(a.length>0?'="'+a+'"':"")+"]"}else i&8?r+="."+s:i&4&&(r+=" "+s);else r!==""&&!bn(s)&&(n+=q_(o,r),r=""),i=s,o=o||!bn(i);e++}return r!==""&&(n+=q_(o,r)),n}function PM(t){return t.map(OM).join(",")}function FM(t){let n=[],e=[],i=1,r=2;for(;i<t.length;){let o=t[i];if(typeof o=="string")r===2?o!==""&&n.push(o,t[++i]):r===8&&e.push(o);else{if(!bn(r))break;r=o}i++}return e.length&&n.push(1,...e),n}var Qt={},Un=(function(t){return t[t.Important=1]="Important",t[t.DashCase=2]="DashCase",t})(Un||{}),LM;function _p(t,n){return LM(t,n)}var Hi=new Set;var V3=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var Vm=new WeakMap;function o0(t){return t?t[Pi]??t:null}var wa=new WeakSet;function VM(t,n,e){let i=Vm.get(t);if(!i||i.length===0)return;let r=n.parentNode,o=n.previousSibling,s=o0(e);for(let a=i.length-1;a>=0;a--){let{el:l,declarationView:c}=i[a],d=l.parentNode;l===n?(i.splice(a,1),wa.add(l),l.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):o&&l===o?(i.splice(a,1),l.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),l.parentNode?.removeChild(l)):d&&r&&d!==r&&(s===null||c===null||s===c)&&(i.splice(a,1),l.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),l.parentNode?.removeChild(l))}}function BM(t,n,e){let i=o0(e),r=Vm.get(t);r?r.some(o=>o.el===n)||r.push({el:n,declarationView:i}):Vm.set(t,[{el:n,declarationView:i}])}var Hd=(function(t){return t[t.CHANGE_DETECTION=0]="CHANGE_DETECTION",t[t.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",t})(Hd||{}),$n=new b(""),Y_=new Set;function $i(t){Y_.has(t)||(Y_.add(t),performance?.mark?.("mark_feature_usage",{detail:{feature:t}}))}var Ud=(()=>{class t{impl=null;execute(){this.impl?.execute()}static \u0275prov=G({token:t,providedIn:"root",factory:()=>new t})}return t})(),yp=[0,1,2,3],Sp=(()=>{class t{ngZone=u(k);scheduler=u(Pn);errorHandler=u(Ot,{optional:!0});sequences=new Set;deferredRegistrations=new Set;executing=!1;constructor(){u($n,{optional:!0})}execute(){let e=this.sequences.size>0;e&&we(_e.AfterRenderHooksStart),this.executing=!0;for(let i of yp)for(let r of this.sequences)if(!(r.erroredOrDestroyed||!r.hooks[i]))try{r.pipelinedValue=this.ngZone.runOutsideAngular(()=>this.maybeTrace(()=>{let o=r.hooks[i];return o(r.pipelinedValue)},r.snapshot))}catch(o){r.erroredOrDestroyed=!0,this.errorHandler?.handleError(o)}this.executing=!1;for(let i of this.sequences)i.afterRun(),i.once&&(this.sequences.delete(i),i.destroy());for(let i of this.deferredRegistrations)this.sequences.add(i);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear(),e&&we(_e.AfterRenderHooksEnd)}register(e){let{view:i}=e;i!==void 0?((i[Lr]??=[]).push(e),Hr(i),i[te]|=8192):this.executing?this.deferredRegistrations.add(e):this.addSequence(e)}addSequence(e){this.sequences.add(e),this.scheduler.notify(7)}unregister(e){this.executing&&this.sequences.has(e)?(e.erroredOrDestroyed=!0,e.pipelinedValue=void 0,e.once=!0):(this.sequences.delete(e),this.deferredRegistrations.delete(e))}maybeTrace(e,i){return i?i.run(Hd.AFTER_NEXT_RENDER,e):e()}static \u0275prov=G({token:t,providedIn:"root",factory:()=>new t})}return t})(),Na=class{impl;hooks;view;once;snapshot;erroredOrDestroyed=!1;pipelinedValue=void 0;unregisterOnDestroy;constructor(n,e,i,r,o,s=null){this.impl=n,this.hooks=e,this.view=i,this.once=r,this.snapshot=s,this.unregisterOnDestroy=o?.onDestroy(()=>this.destroy())}afterRun(){this.erroredOrDestroyed=!1,this.pipelinedValue=void 0,this.snapshot?.dispose(),this.snapshot=null}destroy(){this.impl.unregister(this),this.unregisterOnDestroy?.();let n=this.view?.[Lr];n&&(this.view[Lr]=n.filter(e=>e!==this))}};function Je(t,n){let e=n?.injector??u(R);return $i("NgAfterNextRender"),HM(t,e,n,!0)}function jM(t){return t instanceof Function?[void 0,void 0,t,void 0]:[t.earlyRead,t.write,t.mixedReadWrite,t.read]}function HM(t,n,e,i){let r=n.get(Ud);r.impl??=n.get(Sp);let o=n.get($n,null,{optional:!0}),s=e?.manualCleanup!==!0?n.get(qe):null,a=n.get($o,null,{optional:!0}),l=new Na(r.impl,jM(t),a?.view,i,s,o?.snapshot(null));return r.impl.register(l),l}var Cp=new b("",{factory:()=>{let t=u(Me),n=new Set;return t.onDestroy(()=>n.clear()),{queue:n,isScheduled:!1,scheduler:null,injector:t}}});function s0(t,n,e){let i=t.get(Cp);if(Array.isArray(n))for(let r of n)i.queue.add(r),e?.detachedLeaveAnimationFns?.push(r);else i.queue.add(n),e?.detachedLeaveAnimationFns?.push(n);i.scheduler&&i.scheduler(t)}function UM(t,n){let e=t.get(Cp);if(Array.isArray(n))for(let i of n)e.queue.delete(i);else e.queue.delete(n)}function zM(t,n){let e=t.get(Cp);if(n.detachedLeaveAnimationFns){for(let i of n.detachedLeaveAnimationFns)e.queue.delete(i);n.detachedLeaveAnimationFns=void 0}}function $M(t,n){for(let[e,i]of n)s0(t,i.animateFns)}function Z_(t,n,e,i){let r=t?.[jn]?.enter;n!==null&&r&&r.has(e.index)&&$M(i,r)}function X_(t,n,e,i){try{e.get(ba)}catch{return i(!1)}let r=t?.[jn];r?.enter?.has(n.index)&&UM(e,r.enter.get(n.index).animateFns);let o=GM(t,n,r);if(o.size===0){let s=!1;if(t){let a=[];zd(t,n,a),s=a.length>0}if(!s)return i(!1)}t&&Hi.add(t[Bn]),s0(e,()=>WM(t,n,r||void 0,o,i),r||void 0)}function GM(t,n,e){let i=new Map,r=e?.leave;if(r&&r.has(n.index)&&i.set(n.index,r.get(n.index)),t&&r)for(let[o,s]of r){if(i.has(o))continue;let l=t[Z].data[o].parent;for(;l;){if(l===n){i.set(o,s);break}l=l.parent}}return i}function WM(t,n,e,i,r){let o=[];if(e&&e.leave)for(let[s]of i){if(!e.leave.has(s))continue;let a=e.leave.get(s);for(let l of a.animateFns){let{promise:c}=l();o.push(c)}e.detachedLeaveAnimationFns=void 0}if(t&&zd(t,n,o),o.length>0){let s=e||t?.[jn];if(s){let a=s.running;a&&o.push(a),s.running=Promise.allSettled(o),YM(t,s.running,r)}else Promise.allSettled(o).then(()=>{t&&Hi.delete(t[Bn]),r(!0)})}else t&&Hi.delete(t[Bn]),r(!1)}function zd(t,n,e){if(n.type&12){let r=t[n.index];if(Yt(r))for(let o=je;o<r.length;o++){let s=r[o];s[Z].type===2&&qM(s,e)}}let i=n.child;for(;i;)zd(t,i,e),i=i.next}function qM(t,n){let e=t[jn];if(e&&e.leave)for(let r of e.leave.values())for(let o of r.animateFns){let{promise:s}=o();n.push(s)}let i=t[Z].firstChild;for(;i;)zd(t,i,n),i=i.next}function YM(t,n,e){n.then(()=>{t[jn]?.running===n&&(t[jn].running=void 0,Hi.delete(t[Bn])),e(!0)})}function Go(t,n,e,i,r,o,s,a){if(r!=null){let l,c=!1;Yt(r)?l=r:ni(r)&&(c=!0,r=r[mn]);let d=gt(r);t===0&&i!==null?(Z_(a,i,o,e),s==null?e0(n,i,d):$r(n,i,d,s||null,!0)):t===1&&i!==null?(Z_(a,i,o,e),$r(n,i,d,s||null,!0),VM(o,d,a)):t===2?(a?.[jn]?.leave?.has(o.index)&&BM(o,d,a),wa.delete(d),X_(a,o,e,f=>{if(wa.has(d)){wa.delete(d);return}t0(n,d,c,f)})):t===3&&(wa.delete(d),X_(a,o,e,()=>{n.destroyNode(d)})),l!=null&&oT(n,t,e,l,o,i,s)}}function ZM(t,n){a0(t,n),n[mn]=null,n[pt]=null}function XM(t,n,e,i,r,o){i[mn]=r,i[pt]=n,Gd(t,i,e,1,r,o)}function a0(t,n){n[pn].changeDetectionScheduler?.notify(9),Gd(t,n,n[Fe],2,null,null)}function KM(t){let n=t[Ho];if(!n)return pm(t[Z],t);for(;n;){let e=null;if(ni(n))e=n[Ho];else{let i=n[je];i&&(e=i)}if(!e){for(;n&&!n[qt]&&n!==t;)ni(n)&&pm(n[Z],n),n=n[rt];n===null&&(n=t),ni(n)&&pm(n[Z],n),e=n&&n[qt]}n=e}}function Dp(t,n){let e=t[Br],i=e.indexOf(n);e.splice(i,1)}function $d(t,n){if(jr(n))return;let e=n[Fe];e.destroyNode&&Gd(t,n,e,3,null,null),KM(n)}function pm(t,n){if(jr(n))return;let e=Q(null);try{n[te]&=-129,n[te]|=256,n[Bt]&&Ni(n[Bt]),JM(t,n),QM(t,n),n[Z].type===1&&n[Fe].destroy();let i=n[Fi];if(i!==null&&Yt(n[rt])){i!==n[rt]&&Dp(i,n);let r=n[Vn];r!==null&&r.detachView(t)}Im(n)}finally{Q(e)}}function QM(t,n){let e=t.cleanup,i=n[jo];if(e!==null)for(let s=0;s<e.length-1;s+=2)if(typeof e[s]=="string"){let a=e[s+3];a>=0?i[a]():i[-a].unsubscribe(),s+=2}else{let a=i[e[s+1]];e[s].call(a)}i!==null&&(n[jo]=null);let r=n[Qn];if(r!==null){n[Qn]=null;for(let s=0;s<r.length;s++){let a=r[s];a()}}let o=n[Jn];if(o!==null){n[Jn]=null;for(let s of o)s.destroy()}}function JM(t,n){let e;if(t!=null&&(e=t.destroyHooks)!=null)for(let i=0;i<e.length;i+=2){let r=n[e[i]];if(!(r instanceof Gr)){let o=e[i+1];if(Array.isArray(o))for(let s=0;s<o.length;s+=2){let a=r[o[s]],l=o[s+1];we(_e.LifecycleHookStart,a,l);try{l.call(a)}finally{we(_e.LifecycleHookEnd,a,l)}}else{we(_e.LifecycleHookStart,r,o);try{o.call(r)}finally{we(_e.LifecycleHookEnd,r,o)}}}}}function l0(t,n,e){if(n===null)throw new w(510,!1);return eT(t,n.parent,e)}function eT(t,n,e){let i=n;for(;i!==null&&i.type&168;)n=i,i=n.parent;if(i===null)return e[mn];if(ii(i)){let{encapsulation:r}=t.data[i.directiveStart+i.componentOffset];if(r===yn.None||r===yn.Emulated)return null}return vn(i,e)}function c0(t,n,e){return nT(t,n,e)}function tT(t,n,e){return t.type&40?vn(t,e):null}var nT=tT,K_;function Ep(t,n,e,i){let r=l0(t,i,n),o=n[Fe],s=i.parent||n[pt],a=c0(s,i,n);if(r!=null)if(Array.isArray(e))for(let l=0;l<e.length;l++)W_(o,r,e[l],a,!1);else W_(o,r,e,a,!1);K_!==void 0&&K_(o,i,n,e,r)}function Ia(t,n){if(n!==null){let e=n.type;if(e&3)return vn(n,t);if(e&4)return Bm(-1,t[n.index]);if(e&8){let i=n.child;if(i!==null)return Ia(t,i);{let r=t[n.index];return Yt(r)?Bm(-1,r):gt(r)}}else{if(e&128)return Ia(t,n.next);if(e&32)return _p(n,t)()||gt(t[n.index]);{let i=d0(t,n);if(i!==null){if(Array.isArray(i))return i[0];let r=ei(t[kt]);return Ia(r,i)}else return Ia(t,n.next)}}}return null}function d0(t,n){if(n!==null){let i=t[kt][pt],r=n.projection;return i.projection[r]}return null}function Bm(t,n){let e=je+t+1;if(e<n.length){let i=n[e],r=i[Z].firstChild;if(r!==null)return Ia(i,r)}return n[Vr]}function xp(t,n,e,i,r,o,s){for(;e!=null;){let a=i[Ln];if(e.type===128){e=e.next;continue}let l=i[e.index],c=e.type;if(s&&n===0&&(l&&Yo(gt(l),i),e.flags|=2),!Bd(e))if(c&8)xp(t,n,e.child,i,r,o,!1),Go(n,t,a,r,l,e,o,i);else if(c&32){let d=_p(e,i),f;for(;f=d();)Go(n,t,a,r,f,e,o,i);Go(n,t,a,r,l,e,o,i)}else c&16?u0(t,n,i,e,r,o):Go(n,t,a,r,l,e,o,i);e=s?e.projectionNext:e.next}}function Gd(t,n,e,i,r,o){t.type===3?iT(e,i,n,r,o):xp(e,i,t.firstChild,n,r,o,!1)}function iT(t,n,e,i,r){let s=e[Z].firstChild,a=s.next,l=gt(e[s.index]),c=gt(e[a.index]),d=a.index+1,f=e[d];if(n===1||n===0)i!==null&&(f&&f.hasChildNodes()?$r(t,i,f,r,!0):($r(t,i,l,r,!0),$r(t,i,c,r,!0)));else if(n===2){if(f||(f=document.createDocumentFragment(),e[d]=f),l&&l.parentNode===f)return;let h=l;for(;h!==null;){let p=h.nextSibling;if(f.appendChild(h),h===c)break;h=p}}}function rT(t,n,e){let i=n[Fe],r=l0(t,e,n),o=e.parent||n[pt],s=c0(o,e,n);u0(i,0,n,e,r,s)}function u0(t,n,e,i,r,o){let s=e[kt],l=s[pt].projection[i.projection];if(Array.isArray(l))for(let c=0;c<l.length;c++){let d=l[c];Go(n,t,e[Ln],r,d,i,o,e)}else{let c=l,d=s[rt];Oy(i)&&(c.flags|=128),xp(t,n,c,d,r,o,!0)}}function oT(t,n,e,i,r,o,s){let a=i[Vr],l=gt(i);if(a!==l&&Go(n,t,e,o,a,r,s),(i[te]&4)===0)for(let c=je;c<i.length;c++){let d=i[c];Gd(d[Z],d,t,n,o,a)}}function sT(t,n,e,i,r){if(n)r?t.addClass(e,i):t.removeClass(e,i);else{let o=i.indexOf("-")===-1?void 0:Un.DashCase;r==null?t.removeStyle(e,i,o):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),o|=Un.Important),t.setStyle(e,i,r,o))}}function wp(t,n,e,i,r,o,s,a,l,c,d){let f=Ze+i,h=f+r,p=aT(f,h),S=typeof c=="function"?c():c;return p[Z]={type:t,blueprint:p,template:e,queries:null,viewQuery:a,declTNode:n,data:p.slice().fill(null,f),bindingStartIndex:f,expandoStartIndex:h,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof o=="function"?o():o,pipeRegistry:typeof s=="function"?s():s,firstChild:null,schemas:l,consts:S,incompleteFirstPass:!1,ssrId:d}}function aT(t,n){let e=[];for(let i=0;i<n;i++)e.push(i<t?null:Qt);return e}function lT(t){let n=t.tView;return n===null||n.incompleteFirstPass?t.tView=wp(1,null,t.template,t.decls,t.vars,t.directiveDefs,t.pipeDefs,t.viewQuery,t.schemas,t.consts,t.id):n}function Ip(t,n,e,i,r,o,s,a,l,c,d){let f=n.blueprint.slice();return f[mn]=r,f[te]=i|4|128|8|64|1024,(c!==null||t&&t[te]&2048)&&(f[te]|=2048),$h(f),f[rt]=f[Pi]=t,f[Ye]=e,f[pn]=s||t&&t[pn],f[Fe]=a||t&&t[Fe],f[Ln]=l||t&&t[Ln]||null,f[pt]=o,f[Bn]=XI(),f[Pr]=d,f[Bh]=c,f[kt]=n.type==2?t[kt]:f,f}function cT(t,n,e){let i=vn(n,t),r=lT(e),o=t[pn].rendererFactory,s=Mp(t,Ip(t,r,null,f0(e),i,n,null,o.createRenderer(i,e),null,null,null));return t[n.index]=s}function f0(t){let n=16;return t.signals?n=4096:t.onPush&&(n=64),n}function h0(t,n,e,i){if(e===0)return-1;let r=n.length;for(let o=0;o<e;o++)n.push(i),t.blueprint.push(i),t.data.push(null);return r}function Mp(t,n){return t[Ho]?t[Vh][qt]=n:t[Ho]=n,t[Vh]=n,n}function _(t=1){m0(He(),se(),ri()+t,!1)}function m0(t,n,e,i){if(!i)if((n[te]&3)===3){let o=t.preOrderCheckHooks;o!==null&&yd(n,o,e)}else{let o=t.preOrderHooks;o!==null&&Sd(n,o,0,e)}Li(e)}var Wd=(function(t){return t[t.None=0]="None",t[t.SignalBased=1]="SignalBased",t[t.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",t})(Wd||{});function jm(t,n,e,i){let r=Q(null);try{let[o,s,a]=t.inputs[e],l=null;(s&Wd.SignalBased)!==0&&(l=n[o][et]),l!==null&&l.transformFn!==void 0?i=l.transformFn(i):a!==null&&(i=a.call(n,i)),t.setInput!==null?t.setInput(n,l,i,e,o):by(n,l,o,i)}finally{Q(r)}}function p0(t,n,e,i,r){let o=ri(),s=i&2;try{Li(-1),s&&n.length>Ze&&m0(t,n,Ze,!1);let a=s?_e.TemplateUpdateStart:_e.TemplateCreateStart;we(a,r,e),e(i,r)}finally{Li(o);let a=s?_e.TemplateUpdateEnd:_e.TemplateCreateEnd;we(a,r,e)}}function Tp(t,n,e){gT(t,n,e),(e.flags&64)===64&&vT(t,n,e)}function qd(t,n,e=vn){let i=n.localNames;if(i!==null){let r=n.index+1;for(let o=0;o<i.length;o+=2){let s=i[o+1],a=s===-1?e(n,t):t[s];t[r++]=a}}}function dT(t,n,e,i){let o=i.get(zy,Uy)||e===yn.ShadowDom||e===yn.ExperimentalIsolatedShadowDom,s=t.selectRootElement(n,o);return uT(s),s}function uT(t){fT(t)}var fT=()=>null;function hT(t){return t==="class"?"className":t==="for"?"htmlFor":t==="formaction"?"formAction":t==="innerHtml"?"innerHTML":t==="readonly"?"readOnly":t==="tabindex"?"tabIndex":t}function mT(t,n,e,i,r,o){let s=n[Z];if(Np(t,s,n,e,i)){ii(t)&&pT(n,t.index);return}t.type&3&&(e=hT(e)),g0(t,n,e,i,r,o)}function g0(t,n,e,i,r,o){if(t.type&3){let s=vn(t,n);i=o!=null?o(i,t.value||"",e):i,r.setProperty(s,e,i)}else t.type&12}function pT(t,n){let e=Zt(n,t);e[te]&16||(e[te]|=64)}function gT(t,n,e){let i=e.directiveStart,r=e.directiveEnd;ii(e)&&cT(n,e,t.data[i+e.componentOffset]),t.firstCreatePass||Md(e,n);let o=e.initialInputs;for(let s=i;s<r;s++){let a=t.data[s],l=Ta(n,t,s,e);if(Yo(l,n),o!==null&&ST(n,s-i,l,a,e,o),gn(a)){let c=Zt(e.index,n);c[Ye]=Ta(n,t,s,e)}}}function vT(t,n,e){let i=e.directiveStart,r=e.directiveEnd,o=e.index,s=v_();try{Li(o);for(let a=i;a<r;a++){let l=t.data[a],c=n[a];sd(a),(l.hostBindings!==null||l.hostVars!==0||l.hostAttrs!==null)&&bT(l,c)}}finally{Li(-1),sd(s)}}function bT(t,n){t.hostBindings!==null&&t.hostBindings(1,n)}function v0(t,n){let e=t.directiveRegistry,i=null;if(e)for(let r=0;r<e.length;r++){let o=e[r];r0(n,o.selectors,!1)&&(i??=[],gn(o)?i.unshift(o):i.push(o))}return i}function _T(t,n,e,i,r,o){let s=vn(t,n);yT(n[Fe],s,o,t.value,e,i,r)}function yT(t,n,e,i,r,o,s){if(o==null)s?.(o,i||"",r),t.removeAttribute(n,r,e);else{let a=s==null?Xc(o):s(o,i||"",r);t.setAttribute(n,r,a,e)}}function ST(t,n,e,i,r,o){let s=o[n];if(s!==null)for(let a=0;a<s.length;a+=2){let l=s[a],c=s[a+1];jm(i,e,l,c)}}function b0(t,n,e,i,r){let o=Ze+e,s=n[Z],a=r(s,n,t,i,e);n[o]=a,zo(t,!0);let l=t.type===2;return l?(n0(n[Fe],a,t),(c_()===0||Sa(t))&&Yo(a,n),d_()):Yo(a,n),fd()&&(!l||!Bd(t))&&Ep(s,n,a,t),t}function _0(t){let n=t;return em()?tm():(n=n.parent,zo(n,!1)),n}function CT(t,n){let e=t[Ln];if(!e)return;let i;try{i=e.get(Xt,null)}catch{i=null}i?.(n)}function Np(t,n,e,i,r){let o=t.inputs?.[i],s=t.hostDirectiveInputs?.[i],a=!1;if(s)for(let l=0;l<s.length;l+=2){let c=s[l],d=s[l+1],f=n.data[c];jm(f,e[c],d,r),a=!0}if(o)for(let l of o){let c=e[l],d=n.data[l];jm(d,c,i,r),a=!0}return a}function DT(t,n){let e=Zt(n,t),i=e[Z];ET(i,e);let r=e[mn];r!==null&&e[Pr]===null&&(e[Pr]=$y(r,e[Ln])),we(_e.ComponentStart);try{kp(i,e,e[Ye])}finally{we(_e.ComponentEnd,e[Ye])}}function ET(t,n){for(let e=n.length;e<t.blueprint.length;e++)n.push(t.blueprint[e])}function kp(t,n,e){ld(n);try{let i=t.viewQuery;i!==null&&Tm(1,i,e);let r=t.template;r!==null&&p0(t,n,r,1,e),t.firstCreatePass&&(t.firstCreatePass=!1),n[Vn]?.finishViewCreation(t),t.staticContentQueries&&Gy(t,n),t.staticViewQueries&&Tm(2,t.viewQuery,e);let o=t.components;o!==null&&xT(n,o)}catch(i){throw t.firstCreatePass&&(t.incompleteFirstPass=!0,t.firstCreatePass=!1),i}finally{n[te]&=-5,cd()}}function xT(t,n){for(let e=0;e<n.length;e++)DT(t,n[e])}function Va(t,n,e,i){let r=Q(null);try{let o=n.tView,a=t[te]&4096?4096:16,l=Ip(t,o,e,a,null,n,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),c=t[n.index];l[Fi]=c;let d=t[Vn];return d!==null&&(l[Vn]=d.createEmbeddedView(o)),kp(o,l,e),l}finally{Q(r)}}function Zo(t,n){return!n||n.firstChild===null||Oy(t)}function ka(t,n,e,i,r=!1){if(t.type===3){let o=t.firstChild,s=o.next,a=gt(n[o.index]),l=gt(n[s.index]),c=a;for(;c!==null&&(i.push(c),c!==l);)c=c.nextSibling;return i}for(;e!==null;){if(e.type===128){e=r?e.projectionNext:e.next;continue}let o=n[e.index];if(o!==null)if(Yt(o)){let a=o[Vr];a!==o[mn]&&i.push(gt(o)),o[te]&4||y0(o,i),i.push(a)}else i.push(gt(o));let s=e.type;if(s&8)ka(t,n,e.child,i);else if(s&32){let a=_p(e,n),l;for(;l=a();)i.push(l)}else if(s&16){let a=d0(n,e);if(Array.isArray(a))i.push(...a);else{let l=ei(n[kt]);ka(l[Z],l,a,i,!0)}}e=r?e.projectionNext:e.next}return i}function y0(t,n){for(let e=je;e<t.length;e++){let i=t[e],r=i[Z].firstChild;r!==null&&ka(i[Z],i,r,n)}}function S0(t){if(t[Lr]!==null){for(let n of t[Lr])n.impl.addSequence(n);t[Lr].length=0}}var C0=[];function wT(t){return t[Bt]??IT(t)}function IT(t){let n=C0.pop()??Object.create(TT);return n.lView=t,n}function MT(t){t.lView[Bt]!==t&&(t.lView=null,C0.push(t))}var TT=N(v({},vr),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{Hr(t.lView)},consumerOnSignalRead(){this.lView[Bt]=this}});function NT(t){let n=t[Bt]??Object.create(kT);return n.lView=t,n}var kT=N(v({},vr),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{let n=ei(t.lView);for(;n&&!D0(n[Z]);)n=ei(n);n&&Gh(n)},consumerOnSignalRead(){this.lView[Bt]=this}});function D0(t){return t.type!==2}function E0(t){if(t[Jn]===null)return;let n=!0;for(;n;){let e=!1;for(let i of t[Jn])if(i.dirty&&(e=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()),t[Jn]===null))return;n=e&&!!(t[te]&8192)}}var AT=100;function x0(t,n=0){let i=t[pn].rendererFactory,r=!1;r||i.begin?.();try{RT(t,n)}finally{r||i.end?.()}}function RT(t,n){let e=nm();try{da(!0),Hm(t,n);let i=0;for(;Ca(t);){if(i===AT)throw new w(103,!1);i++,Hm(t,1)}}finally{da(e)}}function OT(t,n,e,i){if(jr(n))return;let r=n[te],o=!1,s=!1;ld(n);let a=!0,l=null,c=null;o||(D0(t)?(c=wT(n),l=Ti(c)):nc()===null?(a=!1,c=NT(n),l=Ti(c)):n[Bt]&&(Ni(n[Bt]),n[Bt]=null));try{$h(n),m_(t.bindingStartIndex),e!==null&&p0(t,n,e,2,i);let d=(r&3)===3;if(!o)if(d){let p=t.preOrderCheckHooks;p!==null&&yd(n,p,null)}else{let p=t.preOrderHooks;p!==null&&Sd(n,p,0,null),hm(n,0)}if(s||PT(n),E0(n),w0(n,0),t.contentQueries!==null&&Gy(t,n),!o)if(d){let p=t.contentCheckHooks;p!==null&&yd(n,p)}else{let p=t.contentHooks;p!==null&&Sd(n,p,1),hm(n,1)}LT(t,n);let f=t.components;f!==null&&M0(n,f,0);let h=t.viewQuery;if(h!==null&&Tm(2,h,i),!o)if(d){let p=t.viewCheckHooks;p!==null&&yd(n,p)}else{let p=t.viewHooks;p!==null&&Sd(n,p,2),hm(n,2)}if(t.firstUpdatePass===!0&&(t.firstUpdatePass=!1),n[td]){for(let p of n[td])p();n[td]=null}o||(S0(n),n[te]&=-73)}catch(d){throw o||Hr(n),d}finally{c!==null&&(_r(c,l),a&&MT(c)),cd()}}function w0(t,n){for(let e=Fy(t);e!==null;e=Ly(e))for(let i=je;i<e.length;i++){let r=e[i];I0(r,n)}}function PT(t){for(let n=Fy(t);n!==null;n=Ly(n)){if(!(n[te]&2))continue;let e=n[Br];for(let i=0;i<e.length;i++){let r=e[i];Gh(r)}}}function FT(t,n,e){we(_e.ComponentStart);let i=Zt(n,t);try{I0(i,e)}finally{we(_e.ComponentEnd,i[Ye])}}function I0(t,n){id(t)&&Hm(t,n)}function Hm(t,n){let i=t[Z],r=t[te],o=t[Bt],s=!!(n===0&&r&16);if(s||=!!(r&64&&n===0),s||=!!(r&1024),s||=!!(o?.dirty&&wo(o)),s||=!1,o&&(o.dirty=!1),t[te]&=-9217,s)OT(i,t,i.template,t[Ye]);else if(r&8192){let a=Q(null);try{E0(t),w0(t,1);let l=i.components;l!==null&&M0(t,l,1),S0(t)}finally{Q(a)}}}function M0(t,n,e){for(let i=0;i<n.length;i++)FT(t,n[i],e)}function LT(t,n){let e=t.hostBindingOpCodes;if(e!==null)try{for(let i=0;i<e.length;i++){let r=e[i];if(r<0)Li(~r);else{let o=r,s=e[++i],a=e[++i];g_(s,o);let l=n[o];we(_e.HostBindingsUpdateStart,l);try{a(2,l)}finally{we(_e.HostBindingsUpdateEnd,l)}}}}finally{Li(-1)}}function Ap(t,n){let e=nm()?64:1088;for(t[pn].changeDetectionScheduler?.notify(n);t;){t[te]|=e;let i=ei(t);if(Uo(t)&&!i)return t;t=i}return null}function T0(t,n,e,i){return[t,!0,0,n,null,i,null,e,null,null]}function N0(t,n){let e=je+n;if(e<t.length)return t[e]}function Ba(t,n,e,i=!0){let r=n[Z];if(VT(r,n,t,e),i){let s=Bm(e,t),a=n[Fe],l=a.parentNode(t[Vr]);l!==null&&XM(r,t[pt],a,n,l,s)}let o=n[Pr];o!==null&&o.firstChild!==null&&(o.firstChild=null)}function k0(t,n){let e=Aa(t,n);return e!==void 0&&$d(e[Z],e),e}function Aa(t,n){if(t.length<=je)return;let e=je+n,i=t[e];if(i){let r=i[Fi];r!==null&&r!==t&&Dp(r,i),n>0&&(t[e-1][qt]=i[qt]);let o=va(t,je+n);ZM(i[Z],i);let s=o[Vn];s!==null&&s.detachView(o[Z]),i[rt]=null,i[qt]=null,i[te]&=-129}return i}function VT(t,n,e,i){let r=je+i,o=e.length;i>0&&(e[r-1][qt]=n),i<o-je?(n[qt]=e[r],Rh(e,je+i,n)):(e.push(n),n[qt]=null),n[rt]=e;let s=n[Fi];s!==null&&e!==s&&A0(s,n);let a=n[Vn];a!==null&&a.insertView(t),rd(n),n[te]|=128}function A0(t,n){let e=t[Br],i=n[rt];if(ni(i))t[te]|=2;else{let r=i[rt][kt];n[kt]!==r&&(t[te]|=2)}e===null?t[Br]=[n]:e.push(n)}var Ui=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let n=this._lView,e=n[Z];return ka(e,n,e.firstChild,[])}constructor(n,e){this._lView=n,this._cdRefInjectingView=e}get context(){return this._lView[Ye]}set context(n){this._lView[Ye]=n}get destroyed(){return jr(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let n=this._lView[rt];if(Yt(n)){let e=n[ya],i=e?e.indexOf(this):-1;i>-1&&(Aa(n,i),va(e,i))}this._attachedToViewContainer=!1}$d(this._lView[Z],this._lView)}onDestroy(n){od(this._lView,n)}markForCheck(){Ap(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[te]&=-129}reattach(){rd(this._lView),this._lView[te]|=128}detectChanges(){this._lView[te]|=1024,x0(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new w(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let n=Uo(this._lView),e=this._lView[Fi];e!==null&&!n&&Dp(e,this._lView),a0(this._lView[Z],this._lView)}attachToAppRef(n){if(this._attachedToViewContainer)throw new w(902,!1);this._appRef=n;let e=Uo(this._lView),i=this._lView[Fi];i!==null&&!e&&A0(i,this._lView),rd(this._lView)}};var Ct=(()=>{class t{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=BT;constructor(e,i,r){this._declarationLView=e,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(e,i){return this.createEmbeddedViewImpl(e,i)}createEmbeddedViewImpl(e,i,r){let o=Va(this._declarationLView,this._declarationTContainer,e,{embeddedViewInjector:i,dehydratedView:r});return new Ui(o)}}return t})();function BT(){return Yd(vt(),se())}function Yd(t,n){return t.type&4?new Ct(n,t,Jo(t,n)):null}function es(t,n,e,i,r){let o=t.data[n];if(o===null)o=jT(t,n,e,i,r),p_()&&(o.flags|=32);else if(o.type&64){o.type=e,o.value=i,o.attrs=r;let s=u_();o.injectorIndex=s===null?-1:s.injectorIndex}return zo(o,!0),o}function jT(t,n,e,i,r){let o=Jh(),s=em(),a=s?o:o&&o.parent,l=t.data[n]=UT(t,a,e,n,i,r);return HT(t,l,o,s),l}function HT(t,n,e,i){t.firstChild===null&&(t.firstChild=n),e!==null&&(i?e.child==null&&n.parent!==null&&(e.child=n):e.next===null&&(e.next=n,n.prev=e))}function UT(t,n,e,i,r,o){let s=n?n.injectorIndex:-1,a=0;return Xh()&&(a|=128),{type:e,index:i,insertBeforeIndex:null,injectorIndex:s,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:a,providerIndexes:0,value:r,namespace:sm(),attrs:o,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:n,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}function zT(t){let n=t[jh]??[],i=t[rt][Fe],r=[];for(let o of n)o.data[Hy]!==void 0?r.push(o):$T(o,i);t[jh]=r}function $T(t,n){let e=0,i=t.firstChild;if(i){let r=t.data[jy];for(;e<r;){let o=i.nextSibling;t0(n,i,!1),i=o,e++}}}var GT=()=>null,WT=()=>null;function Td(t,n){return GT(t,n)}function R0(t,n,e){return WT(t,n,e)}var O0=class{},Qe=class{},Re=class{destroyNode=null;static __NG_ELEMENT_ID__=()=>qT()};function qT(){let t=se(),n=vt(),e=Zt(n.index,t);return(ni(e)?e:t)[Fe]}var P0=(()=>{class t{static \u0275prov=G({token:t,providedIn:"root",factory:()=>null})}return t})();function F0(t){return t.debugInfo?.className||t.type.name||null}var Dd={},Nd=class{injector;parentInjector;constructor(n,e){this.injector=n,this.parentInjector=e}get(n,e,i){let r=this.injector.get(n,Dd,i);return r!==Dd||e===Dd?r:this.parentInjector.get(n,e,i)}};function YT(t,n,e){return t[n]=e}function ZT(t,n){return t[n]}function Gi(t,n,e){if(e===Qt)return!1;let i=t[n];return Object.is(i,e)?!1:(t[n]=e,!0)}function Ed(t,n,e){return function i(r){let o=i.__ngNativeEl__;o!==void 0&&QI(r,o);let s=ii(t)?Zt(t.index,n):n;Ap(s,5);let a=n[Ye],l=Q_(n,a,e,r),c=i.__ngNextListenerFn__;for(;c;)l=Q_(n,a,c,r)&&l,c=c.__ngNextListenerFn__;return l}}function Q_(t,n,e,i){let r=Q(null);try{return we(_e.OutputStart,n,e),e(i)!==!1}catch(o){return CT(t,o),!1}finally{we(_e.OutputEnd,n,e),Q(r)}}function L0(t,n,e,i,r,o,s,a){let l=Sa(t),c=!1,d=null;if(!i&&l&&(d=KT(n,e,o,t.index)),d!==null){let f=d.__ngLastListenerFn__||d;f.__ngNextListenerFn__=s,d.__ngLastListenerFn__=s,c=!0}else{let f=vn(t,e),h=i?i(f):f;eM(e,h,o,a),i||(a.__ngNativeEl__=f);let p=r.listen(h,o,a);if(!XT(o)){let S=i?E=>i(gt(E[t.index])):t.index;V0(S,n,e,o,a,p,!1)}}return c}function XT(t){return t.startsWith("animation")||t.startsWith("transition")}function KT(t,n,e,i){let r=t.cleanup;if(r!=null)for(let o=0;o<r.length-1;o+=2){let s=r[o];if(s===e&&r[o+1]===i){let a=n[jo],l=r[o+2];return a&&a.length>l?a[l]:null}typeof s=="string"&&(o+=2)}return null}function V0(t,n,e,i,r,o,s){let a=n.firstCreatePass?qh(n):null,l=Wh(e),c=l.length;l.push(r,o),a&&a.push(i,t,c,(c+1)*(s?-1:1))}function J_(t,n,e,i,r,o){let s=n[e],a=n[Z],c=a.data[e].outputs[i],f=s[c].subscribe(o);V0(t.index,a,n,r,o,f,!0)}var Um=Symbol("BINDING");var Yr=new b("");function kd(t,n,e){let i=e?t.styles:null,r=e?t.classes:null,o=0;if(n!==null)for(let s=0;s<n.length;s++){let a=n[s];if(typeof a=="number")o=a;else if(o==1)r=Wc(r,a);else if(o==2){let l=a,c=n[++s];i=Wc(i,l+": "+c+";")}}e?t.styles=i:t.stylesWithoutHost=i,e?t.classes=r:t.classesWithoutHost=r}function ft(t,n=0){let e=se();if(e===null)return A(t,n);let i=vt();return Ny(i,e,it(t),n)}function Zd(){let t="invalid";throw new Error(t)}function B0(t,n,e,i,r){let o=i===null?null:{"":-1},s=r(t,e);if(s!==null){let a=s,l=null,c=null;for(let d of s)if(d.resolveHostDirectives!==null){[a,l,c]=d.resolveHostDirectives(s);break}eN(t,n,e,a,o,l,c)}o!==null&&i!==null&&QT(e,i,o)}function QT(t,n,e){let i=t.localNames=[];for(let r=0;r<n.length;r+=2){let o=e[n[r+1]];if(o==null)throw new w(-301,!1);i.push(n[r],o)}}function JT(t,n,e){n.componentOffset=e,(t.components??=[]).push(n.index)}function eN(t,n,e,i,r,o,s){let a=i.length,l=null;for(let h=0;h<a;h++){let p=i[h];l===null&&gn(p)&&(l=p,JT(t,e,h)),xm(Md(e,n),t,p.type)}sN(e,t.data.length,a),l?.viewProvidersResolver&&l.viewProvidersResolver(l);for(let h=0;h<a;h++){let p=i[h];p.providersResolver&&p.providersResolver(p)}let c=!1,d=!1,f=h0(t,n,a,null);a>0&&(e.directiveToIndex=new Map);for(let h=0;h<a;h++){let p=i[h];if(e.mergedAttrs=qo(e.mergedAttrs,p.hostAttrs),nN(t,e,n,f,p),oN(f,p,r),s!==null&&s.has(p)){let[E,$]=s.get(p);e.directiveToIndex.set(p.type,[f,E+e.directiveStart,$+e.directiveStart])}else(o===null||!o.has(p))&&e.directiveToIndex.set(p.type,f);p.contentQueries!==null&&(e.flags|=4),(p.hostBindings!==null||p.hostAttrs!==null||p.hostVars!==0)&&(e.flags|=64);let S=p.type.prototype;!c&&(S.ngOnChanges||S.ngOnInit||S.ngDoCheck)&&((t.preOrderHooks??=[]).push(e.index),c=!0),!d&&(S.ngOnChanges||S.ngDoCheck)&&((t.preOrderCheckHooks??=[]).push(e.index),d=!0),f++}tN(t,e,o)}function tN(t,n,e){for(let i=n.directiveStart;i<n.directiveEnd;i++){let r=t.data[i];if(e===null||!e.has(r))ey(0,n,r,i),ey(1,n,r,i),ny(n,i,!1);else{let o=e.get(r);ty(0,n,o,i),ty(1,n,o,i),ny(n,i,!0)}}}function ey(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(Object.hasOwn(r,o)){let s;t===0?s=n.inputs??={}:s=n.outputs??={},s[o]??=[],s[o].push(i),j0(n,o)}}function ty(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(Object.hasOwn(r,o)){let s=r[o],a;t===0?a=n.hostDirectiveInputs??={}:a=n.hostDirectiveOutputs??={},a[s]??=[],a[s].push(i,o),j0(n,s)}}function j0(t,n){n==="class"?t.flags|=8:n==="style"&&(t.flags|=16)}function ny(t,n,e){let{attrs:i,inputs:r,hostDirectiveInputs:o}=t;if(i===null||!e&&r===null||e&&o===null||bp(t)){t.initialInputs??=[],t.initialInputs.push(null);return}let s=null,a=0;for(;a<i.length;){let l=i[a];if(l===0){a+=4;continue}else if(l===5){a+=2;continue}else if(typeof l=="number")break;if(!e&&Object.hasOwn(r,l)){let c=r[l];for(let d of c)if(d===n){s??=[],s.push(l,i[a+1]);break}}else if(e&&Object.hasOwn(o,l)){let c=o[l];for(let d=0;d<c.length;d+=2)if(c[d]===n){s??=[],s.push(c[d+1],i[a+1]);break}}a+=2}t.initialInputs??=[],t.initialInputs.push(s)}function nN(t,n,e,i,r){t.data[i]=r;let o=r.factory||(r.factory=Nr(r.type,!0)),s=new Gr(o,gn(r),ft,null);t.blueprint[i]=s,e[i]=s,iN(t,n,i,h0(t,e,r.hostVars,Qt),r)}function iN(t,n,e,i,r){let o=r.hostBindings;if(o){let s=t.hostBindingOpCodes;s===null&&(s=t.hostBindingOpCodes=[]);let a=~n.index;rN(s)!=a&&s.push(a),s.push(e,i,o)}}function rN(t){let n=t.length;for(;n>0;){let e=t[--n];if(typeof e=="number"&&e<0)return e}return 0}function oN(t,n,e){if(e){if(n.exportAs)for(let i=0;i<n.exportAs.length;i++)e[n.exportAs[i]]=t;gn(n)&&(e[""]=t)}}function sN(t,n,e){t.flags|=1,t.directiveStart=n,t.directiveEnd=n+e,t.providerIndexes=n}function H0(t,n,e,i,r,o,s,a){let l=n[Z],c=l.consts,d=jt(c,s),f=es(l,t,e,i,d);return o&&B0(l,n,f,jt(c,a),r),f.mergedAttrs=qo(f.mergedAttrs,f.attrs),f.attrs!==null&&kd(f,f.attrs,!1),f.mergedAttrs!==null&&kd(f,f.mergedAttrs,!0),l.queries!==null&&l.queries.elementStart(l,f),f}function U0(t,n){Sy(t,n),Hh(n)&&t.queries.elementEnd(n)}function aN(t,n,e,i,r,o){let s=n.consts,a=jt(s,r),l=es(n,t,e,i,a);if(l.mergedAttrs=qo(l.mergedAttrs,l.attrs),o!=null){let c=jt(s,o);l.localNames=[];for(let d=0;d<c.length;d+=2)l.localNames.push(c[d],-1)}return l.attrs!==null&&kd(l,l.attrs,!1),l.mergedAttrs!==null&&kd(l,l.mergedAttrs,!0),n.queries!==null&&n.queries.elementStart(n,l),l}var z0=typeof ShadowRoot<"u",lN=typeof Document<"u";function cN(t){return Object.keys(t).map(n=>{let[e,i,r]=t[n],o={propName:e,templateName:n,isSignal:(i&Wd.SignalBased)!==0};return r&&(o.transform=r),o})}function dN(t){return Object.keys(t).map(n=>({propName:t[n],templateName:n}))}function uN(t,n,e){let i=n instanceof Me?n:n?.injector;return i&&t.getStandaloneInjector!==null&&(i=t.getStandaloneInjector(i)||i),i?new Nd(e,i):e}function fN(t){let n=t.get(Qe,null);if(n===null)throw new w(407,!1);let e=t.get(P0,null),i=t.get(Pn,null),r=t.get($n,null,{optional:!0});return{rendererFactory:n,sanitizer:e,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function hN(t,n,e){let i=$0(t);return Jy(n,i,i==="svg"?Uh:i==="math"?n_:e)}function mN(t){if((t&&"localName"in t&&typeof t.localName=="string"?t.localName:t?.tagName)?.toLowerCase()==="script")throw new w(905,!1)}function $0(t){return(t.selectors[0][0]||"div").toLowerCase()}var Xo=class{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=cN(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=dN(this.componentDef.outputs),this.cachedOutputs}constructor(n,e){this.componentDef=n,this.ngModule=e,this.componentType=n.type,this.selector=PM(n.selectors),this.ngContentSelectors=n.ngContentSelectors??[],this.isBoundToModule=!!e}create(n,e,i,r,o,s,a){we(_e.DynamicComponentStart);let l=Q(null);try{let c=this.componentDef,d=uN(c,r||this.ngModule,n),f=fN(d),h=f.tracingService;return h&&h.componentCreate?h.componentCreate(F0(c),()=>this.createComponentRef(f,d,e,i,o,s,a)):this.createComponentRef(f,d,e,i,o,s,a)}finally{Q(l)}}createComponentRef(n,e,i,r,o,s,a){let l=this.componentDef,c=pN(r,l,s,o),d=n.rendererFactory.createRenderer(null,l),f=r?dT(d,r,l.encapsulation,e):hN(l,d,a??null);mN(f);let h=e.get(Yr,null),p=gN(f,()=>e.get(j,null)??By());h&&h.addHost(p);let S=s?.some(iy)||o?.some(H=>typeof H!="function"&&H.bindings.some(iy)),E=Ip(null,c,null,512|f0(l),null,null,n,d,e,null,$y(f,e,!0));h&&z0&&p instanceof ShadowRoot&&od(E,()=>{h.removeHost(p)}),E[Ze]=f,ld(E);let $=null;try{let H=H0(Ze,E,2,"#host",()=>c.directiveRegistry,!0,0);n0(d,f,H),Yo(f,E),Tp(c,E,H),Wy(c,H,E),U0(c,H),i!==void 0&&bN(H,this.ngContentSelectors,i),$=Zt(H.index,E),E[Ye]=$[Ye],kp(c,E,null)}catch(H){throw $!==null&&Im($),Im(E),H}finally{we(_e.DynamicComponentEnd),cd()}return new Ad(this.componentType,E,!!S)}};function pN(t,n,e,i){let r=t?["ng-version","22.1.6"]:FM(n.selectors[0]),o=null,s=null,a=0;if(e)for(let d of e)a+=d[Um].requiredVars,d.create&&(d.targetIdx=0,(o??=[]).push(d)),d.update&&(d.targetIdx=0,(s??=[]).push(d));if(i)for(let d=0;d<i.length;d++){let f=i[d];if(typeof f!="function")for(let h of f.bindings){a+=h[Um].requiredVars;let p=d+1;h.create&&(h.targetIdx=p,(o??=[]).push(h)),h.update&&(h.targetIdx=p,(s??=[]).push(h))}}let l=[n];if(i)for(let d of i){let f=typeof d=="function"?d:d.type,h=Yc(f);l.push(h)}return wp(0,null,vN(o,s),1,a,l,null,null,null,[r],null)}function gN(t,n){let e=t.getRootNode?.();return lN&&e instanceof Document?e.head:e&&z0&&e instanceof ShadowRoot?e:n().head}function vN(t,n){return!t&&!n?null:e=>{if(e&1&&t)for(let i of t)i.create();if(e&2&&n)for(let i of n)i.update()}}function iy(t){let n=t[Um].kind;return n==="input"||n==="twoWay"}var Ad=class extends O0{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(n,e,i){super(),this._rootLView=e,this._hasInputBindings=i,this._tNode=nd(e[Z],Ze),this.location=Jo(this._tNode,e),this.instance=Zt(this._tNode.index,e)[Ye],this.hostView=this.changeDetectorRef=new Ui(e,void 0),this.componentType=n}setInput(n,e){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(n)&&Object.is(this.previousInputValues.get(n),e))return;let r=this._rootLView,o=Np(i,r[Z],r,n,e);this.previousInputValues.set(n,e);let s=Zt(i.index,r);Ap(s,1)}get injector(){return new ji(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(n){this.hostView.onDestroy(n)}};function bN(t,n,e){let i=t.projection=[];for(let r=0;r<n.length;r++){let o=e[r];i.push(o!=null&&o.length?Array.from(o):null)}}var bt=(()=>{class t{static __NG_ELEMENT_ID__=_N}return t})();function _N(){let t=vt();return G0(t,se())}var zm=class t extends bt{_lContainer;_hostTNode;_hostLView;constructor(n,e,i){super(),this._lContainer=n,this._hostTNode=e,this._hostLView=i}get element(){return Jo(this._hostTNode,this._hostLView)}get injector(){return new ji(this._hostTNode,this._hostLView)}get parentInjector(){let n=ap(this._hostTNode,this._hostLView);if(Ey(n)){let e=Id(n,this._hostLView),i=wd(n),r=e[Z].data[i+8];return new ji(r,e)}else return new ji(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(n){let e=ry(this._lContainer);return e!==null&&e[n]||null}get length(){return this._lContainer.length-je}createEmbeddedView(n,e,i){let r,o;typeof i=="number"?r=i:i!=null&&(r=i.index,o=i.injector);let s=Td(this._lContainer,n.ssrId),a=n.createEmbeddedViewImpl(e||{},o,s);return this.insertImpl(a,r,Zo(this._hostTNode,s)),a}createComponent(n,e,i,r,o,s,a){let l,c=e||{};l=c.index,i=c.injector,r=c.projectableNodes,o=c.environmentInjector||c.ngModuleRef,s=c.directives,a=c.bindings;let d=new Xo(Ri(n)),f=i||this.parentInjector;if(!o&&d.ngModule==null){let H=this.parentInjector.get(Me,null);H&&(o=H)}let h=Ri(d.componentType??{}),p=Td(this._lContainer,h?.id??null),S=p?.firstChild??null,E=d.create(f,r,S,o,s,a,this._getHostElementNamespace());return this.insertImpl(E.hostView,l,Zo(this._hostTNode,p)),E}_getHostElementNamespace(){if(this._hostTNode.type&2){let n=this._hostTNode.parent??this._hostLView[pt];return n!==null&&n.type&2&&typeof n.value=="string"&&n.value.toLowerCase()==="foreignobject"?null:n?.namespace??null}return this._hostTNode.namespace}insert(n,e){return this.insertImpl(n,e,!0)}insertImpl(n,e,i){let r=n._lView;if(o_(r)){let a=this.indexOf(n);if(a!==-1)this.detach(a);else{let l=r[rt],c=new t(l,l[pt],l[rt]);c.detach(c.indexOf(n))}}let o=this._adjustIndex(e),s=this._lContainer;return Ba(s,r,o,i),n.attachToViewContainerRef(),Rh(gm(s),o,n),n}move(n,e){return this.insert(n,e)}indexOf(n){let e=ry(this._lContainer);return e!==null?e.indexOf(n):-1}remove(n){let e=this._adjustIndex(n,-1),i=Aa(this._lContainer,e);i&&(va(gm(this._lContainer),e),$d(i[Z],i))}detach(n){let e=this._adjustIndex(n,-1),i=Aa(this._lContainer,e);return i&&va(gm(this._lContainer),e)!=null?new Ui(i):null}_adjustIndex(n,e=0){return n??this.length+e}};function ry(t){return t[ya]}function gm(t){return t[ya]||(t[ya]=[])}function G0(t,n){let e,i=n[t.index];return Yt(i)?e=i:(e=T0(i,n,null,t),n[t.index]=e,Mp(n,e)),SN(e,n,t,i),new zm(e,t,n)}function yN(t,n){let e=t[Fe],i=e.createComment(""),r=vn(n,t),o=e.parentNode(r);return $r(e,o,i,e.nextSibling(r),!1),i}var SN=EN,CN=()=>!1;function DN(t,n,e){return CN(t,n,e)}function EN(t,n,e,i){if(t[Vr])return;let r;e.type&8?r=gt(i):r=yN(n,e),t[Vr]=r}var $m=class t{queryList;matches=null;constructor(n){this.queryList=n}clone(){return new t(this.queryList)}setDirty(){this.queryList.setDirty()}},Gm=class t{queries;constructor(n=[]){this.queries=n}createEmbeddedView(n){let e=n.queries;if(e!==null){let i=n.contentQueries!==null?n.contentQueries[0]:e.length,r=[];for(let o=0;o<i;o++){let s=e.getByIndex(o),a=this.queries[s.indexInDeclarationView];r.push(a.clone())}return new t(r)}return null}insertView(n){this.dirtyQueriesWithMatches(n)}detachView(n){this.dirtyQueriesWithMatches(n)}finishViewCreation(n){this.dirtyQueriesWithMatches(n)}dirtyQueriesWithMatches(n){for(let e=0;e<this.queries.length;e++)Op(n,e).matches!==null&&this.queries[e].setDirty()}},Rd=class{flags;read;predicate;constructor(n,e,i=null){this.flags=e,this.read=i,typeof n=="string"?this.predicate=TN(n):this.predicate=n}},Wm=class t{queries;constructor(n=[]){this.queries=n}elementStart(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(n,e)}elementEnd(n){for(let e=0;e<this.queries.length;e++)this.queries[e].elementEnd(n)}embeddedTView(n){let e=null;for(let i=0;i<this.length;i++){let r=e!==null?e.length:0,o=this.getByIndex(i).embeddedTView(n,r);o&&(o.indexInDeclarationView=i,e!==null?e.push(o):e=[o])}return e!==null?new t(e):null}template(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].template(n,e)}getByIndex(n){return this.queries[n]}get length(){return this.queries.length}track(n){this.queries.push(n)}},qm=class t{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(n,e=-1){this.metadata=n,this._declarationNodeIndex=e}elementStart(n,e){this.isApplyingToNode(e)&&this.matchTNode(n,e)}elementEnd(n){this._declarationNodeIndex===n.index&&(this._appliesToNextNode=!1)}template(n,e){this.elementStart(n,e)}embeddedTView(n,e){return this.isApplyingToNode(n)?(this.crossesNgTemplate=!0,this.addMatch(-n.index,e),new t(this.metadata)):null}isApplyingToNode(n){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let e=this._declarationNodeIndex,i=n.parent;for(;i!==null&&i.type&8&&i.index!==e;)i=i.parent;return e===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(n,e){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let o=i[r];this.matchTNodeWithReadOption(n,e,xN(e,o)),this.matchTNodeWithReadOption(n,e,Cd(e,n,o,!1,!1))}else i===Ct?e.type&4&&this.matchTNodeWithReadOption(n,e,-1):this.matchTNodeWithReadOption(n,e,Cd(e,n,i,!1,!1))}matchTNodeWithReadOption(n,e,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===O||r===bt||r===Ct&&e.type&4)this.addMatch(e.index,-2);else{let o=Cd(e,n,r,!1,!1);o!==null&&this.addMatch(e.index,o)}else this.addMatch(e.index,i)}}addMatch(n,e){this.matches===null?this.matches=[n,e]:this.matches.push(n,e)}};function xN(t,n){let e=t.localNames;if(e!==null){for(let i=0;i<e.length;i+=2)if(e[i]===n)return e[i+1]}return null}function wN(t,n){return t.type&11?Jo(t,n):t.type&4?Yd(t,n):null}function IN(t,n,e,i){return e===-1?wN(n,t):e===-2?MN(t,n,i):Ta(t,t[Z],e,n)}function MN(t,n,e){if(e===O)return Jo(n,t);if(e===Ct)return Yd(n,t);if(e===bt)return G0(n,t)}function W0(t,n,e,i){let r=n[Vn].queries[i];if(r.matches===null){let o=t.data,s=e.matches,a=[];for(let l=0;s!==null&&l<s.length;l+=2){let c=s[l];if(c<0)a.push(null);else{let d=o[c];a.push(IN(n,d,s[l+1],e.metadata.read))}}r.matches=a}return r.matches}function Ym(t,n,e,i){let r=t.queries.getByIndex(e),o=r.matches;if(o!==null){let s=W0(t,n,r,e);for(let a=0;a<o.length;a+=2){let l=o[a];if(l>0)i.push(s[a/2]);else{let c=o[a+1],d=n[-l];for(let f=je;f<d.length;f++){let h=d[f];h[Fi]===h[rt]&&Ym(h[Z],h,c,i)}if(d[Br]!==null){let f=d[Br];for(let h=0;h<f.length;h++){let p=f[h];Ym(p[Z],p,c,i)}}}}}return i}function Rp(t,n){return t[Vn].queries[n].queryList}function q0(t,n,e){let i=new ai((e&4)===4);return l_(t,n,i,i.destroy),(n[Vn]??=new Gm).queries.push(new $m(i))-1}function Y0(t,n,e){let i=He();return i.firstCreatePass&&(X0(i,new Rd(t,n,e),-1),(n&2)===2&&(i.staticViewQueries=!0)),q0(i,se(),n)}function Z0(t,n,e,i){let r=He();if(r.firstCreatePass){let o=vt();X0(r,new Rd(n,e,i),o.index),NN(r,t),(e&2)===2&&(r.staticContentQueries=!0)}return q0(r,se(),e)}function TN(t){return t.split(",").map(n=>n.trim())}function X0(t,n,e){t.queries===null&&(t.queries=new Wm),t.queries.track(new qm(n,e))}function NN(t,n){let e=t.contentQueries||(t.contentQueries=[]),i=e.length?e[e.length-1]:-1;n!==i&&e.push(t.queries.length-1,n)}function Op(t,n){return t.queries.getByIndex(n)}function K0(t,n){let e=t[Z],i=Op(e,n);return i.crossesNgTemplate?Ym(e,t,n,[]):W0(e,t,i,n)}function Q0(t,n,e){let i,r=Xs(()=>{i._dirtyCounter();let o=kN(i,t);if(n&&o===void 0)throw new w(-951,!1);return o});return i=r[et],i._dirtyCounter=U(0),i._flatValue=void 0,r}function Pp(t){return Q0(!0,!1,t)}function Fp(t){return Q0(!0,!0,t)}function J0(t,n){let e=t[et];e._lView=se(),e._queryIndex=n,e._queryList=Rp(e._lView,n),e._queryList.onDirty(()=>e._dirtyCounter.update(i=>i+1))}function kN(t,n){let e=t._lView,i=t._queryIndex;if(e===void 0||i===void 0||e[te]&4)return n?void 0:St;let r=Rp(e,i),o=K0(e,i);return r.reset(o,Ry),n?r.first:r._changesDetected||t._flatValue===void 0?t._flatValue=r.toArray():t._flatValue}function di(t){return!!t&&typeof t.then=="function"}function Lp(t){return!!t&&typeof t.subscribe=="function"}var zn=class{},Xd=class{};var Od=class extends zn{ngModuleType;_parent;_bootstrapComponents=[];_r3Injector;instance;destroyCbs=[];constructor(n,e,i,r=!0){super(),this.ngModuleType=n,this._parent=e;let o=jb(n);this._bootstrapComponents=EM(o.bootstrap),this._r3Injector=am(n,e,[{provide:zn,useValue:this},...i],ha(n),new Set(["environment"])),r&&this.resolveInjectorInitializers()}resolveInjectorInitializers(){this._r3Injector.resolveInjectorInitializers(),this.instance=this._r3Injector.get(this.ngModuleType)}get injector(){return this._r3Injector}destroy(){let n=this._r3Injector;!n.destroyed&&n.destroy(),this.destroyCbs.forEach(e=>e()),this.destroyCbs=null}onDestroy(n){this.destroyCbs.push(n)}},Pd=class extends Xd{moduleType;constructor(n){super(),this.moduleType=n}create(n){return new Od(this.moduleType,n,[])}};var Ra=class extends zn{injector;instance=null;constructor(n){super();let e=new Ar([...n.providers,{provide:zn,useValue:this}],n.parent||Bo(),n.debugName,new Set(["environment"]));this.injector=e,n.runEnvironmentInitializers&&e.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(n){this.injector.onDestroy(n)}};function ja(t,n,e=null){return new Ra({providers:t,parent:n,debugName:e,runEnvironmentInitializers:!0}).injector}var AN=(()=>{class t{_injector;cachedInjectors=new Map;constructor(e){this._injector=e}getOrCreateStandaloneInjector(e){if(!e.standalone)return null;if(!this.cachedInjectors.has(e)){let i=Ph(!1,e.type),r=i.length>0?ja([i],this._injector,""):null;this.cachedInjectors.set(e,r)}return this.cachedInjectors.get(e)}ngOnDestroy(){try{for(let e of this.cachedInjectors.values())e!==null&&e.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=G({token:t,providedIn:"environment",factory:()=>new t(A(Me))})}return t})();function T(t){return Pa(()=>{let n=eS(t),e=N(v({},n),{decls:t.decls,vars:t.vars,template:t.template,consts:t.consts||null,ngContentSelectors:t.ngContentSelectors,onPush:t.changeDetection!==cp.Eager,directiveDefs:null,pipeDefs:null,dependencies:n.standalone&&t.dependencies||null,getStandaloneInjector:n.standalone?r=>r.get(AN).getOrCreateStandaloneInjector(e):null,getExternalStyles:null,signals:t.signals??!1,data:t.data||{},encapsulation:t.encapsulation||yn.Emulated,styles:t.styles||St,_:null,schemas:t.schemas||null,tView:null,id:""});n.standalone&&$i("NgStandalone"),tS(e);let i=t.dependencies;return e.directiveDefs=oy(i,RN),e.pipeDefs=oy(i,Hb),e.id=FN(e),e})}function RN(t){return Ri(t)||Yc(t)}function X(t){return Pa(()=>({type:t.type,bootstrap:t.bootstrap||St,declarations:t.declarations||St,imports:t.imports||St,exports:t.exports||St,transitiveCompileScopes:null,schemas:t.schemas||null,id:t.id||null}))}function ON(t,n){if(t==null)return Oi;let e={};for(let i in t)if(Object.hasOwn(t,i)){let r=t[i],o,s,a,l;Array.isArray(r)?(a=r[0],o=r[1],s=r[2]??o,l=r[3]||null):(o=r,s=r,a=Wd.None,l=null),e[o]=[i,a,l],n[o]=s}return e}function PN(t){if(t==null)return Oi;let n={};for(let e in t)Object.hasOwn(t,e)&&(n[t[e]]=e);return n}function z(t){return Pa(()=>{let n=eS(t);return tS(n),n})}function eS(t){let n={};return{type:t.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:t.hostBindings||null,hostVars:t.hostVars||0,hostAttrs:t.hostAttrs||null,contentQueries:t.contentQueries||null,declaredInputs:n,inputConfig:t.inputs||Oi,exportAs:t.exportAs||null,standalone:t.standalone??!0,signals:t.signals===!0,selectors:t.selectors||St,viewQuery:t.viewQuery||null,features:t.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,signalFormsInputPresence:null,inputs:ON(t.inputs,n),outputs:PN(t.outputs),debugInfo:null}}function tS(t){t.features?.forEach(n=>n(t))}function oy(t,n){return t?()=>{let e=typeof t=="function"?t():t,i=[];for(let r of e){let o=n(r);o!==null&&i.push(o)}return i}:null}function FN(t){let n=0,e=typeof t.consts=="function"?"":t.consts,i=[t.selectors,t.ngContentSelectors,t.hostVars,t.hostAttrs,e,t.vars,t.decls,t.encapsulation,t.standalone,t.signals,t.exportAs,JSON.stringify(t.inputs),JSON.stringify(t.outputs),Object.getOwnPropertyNames(t.type.prototype),!!t.contentQueries,!!t.viewQuery];for(let o of i.join("|"))n=Math.imul(31,n)+o.charCodeAt(0)<<0;return n+=2147483648,"c"+n}var nS=new b("");var Vp=(()=>{class t{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((e,i)=>{this.resolve=e,this.reject=i});appInits=u(nS,{optional:!0})??[];injector=u(R);constructor(){}runInitializers(){if(this.initialized)return;let e=[];for(let r of this.appInits){let o=ot(this.injector,r);if(di(o))e.push(o);else if(Lp(o)){let s=new Promise((a,l)=>{o.subscribe({complete:a,error:l})});e.push(s)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(e).then(()=>{i()}).catch(r=>{this.reject(r)}),e.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();function Bp(t){let n=e=>{let i=Array.isArray(t);e.hostDirectives===null?(e.resolveHostDirectives=LN,e.hostDirectives=i?t.map(Zm):[t]):i?e.hostDirectives.unshift(...t.map(Zm)):e.hostDirectives.unshift(t)};return n.ngInherit=!0,n}function LN(t){let n=[],e=!1,i=null,r=null;for(let o=0;o<t.length;o++){let s=t[o];if(s.hostDirectives!==null){let a=n.length;i??=new Map,r??=new Map,iS(s,n,i,t),r.set(s,[a,n.length-1])}o===0&&gn(s)&&(e=!0,n.push(s))}for(let o=e?1:0;o<t.length;o++)n.push(t[o]);return i!==null&&i.forEach((o,s)=>{VN(s.declaredInputs,o.inputs)}),[n,i,r]}function iS(t,n,e,i){if(t.hostDirectives!==null)for(let r of t.hostDirectives)if(typeof r=="function"){let o=r();for(let s of o)sy(Zm(s),n,e,i)}else sy(r,n,e,i)}function sy(t,n,e,i){let r=Yc(t.directive);if(iS(r,n,e,i),e.has(r)){let o=e.get(r);ay(o,t.inputs,"input"),ay(o,t.outputs,"output")}else i.includes(r)||(e.set(r,t),n.push(r))}function ay(t,n,e){let i=e==="input"?t.inputs:t.outputs;Object.keys(n).forEach(r=>{let o=n[r];(!Object.hasOwn(i,r)||i[r]===o)&&(i[r]=o)})}function Zm(t){return typeof t=="function"?{directive:it(t),inputs:{},outputs:{}}:{directive:it(t.directive),inputs:ly(t.inputs),outputs:ly(t.outputs)}}function ly(t){let n={};if(t!==void 0&&t.length>0)for(let e=0;e<t.length;e+=2)n[t[e]]=t[e+1];return n}function VN(t,n){for(let e in n)if(Object.hasOwn(n,e)){let i=n[e],r=t[e];t[i]=r}}function BN(t){return Object.getPrototypeOf(t.prototype).constructor}function Te(t){let n=BN(t.type),e=!0,i=[t];for(;n&&n!==Function.prototype&&n!==Object.prototype;){let r,o=Object.hasOwn(n,pa)?n[pa]:void 0,s=Object.hasOwn(n,ga)?n[ga]:void 0;if(gn(t))r=o??s;else{if(o)throw new w(903,!1);r=s}if(r){if(e){i.push(r);let l=t;l.inputs=vm(t.inputs),l.declaredInputs=vm(t.declaredInputs),l.outputs=vm(t.outputs);let c=r.hostBindings;c&&$N(t,c);let d=r.viewQuery,f=r.contentQueries;if(d&&UN(t,d),f&&zN(t,f),jN(t,r),Bb(t.outputs,r.outputs),gn(r)&&r.data.animation){let h=t.data;h.animation=(h.animation||[]).concat(r.data.animation)}}let a=r.features;if(a)for(let l=0;l<a.length;l++){let c=a[l];c&&c.ngInherit&&c(t),c===Te&&(e=!1)}}n=Object.getPrototypeOf(n)}HN(i)}function jN(t,n){for(let e in n.inputs){if(!Object.hasOwn(n.inputs,e)||Object.hasOwn(t.inputs,e))continue;let i=n.inputs[e];i!==void 0&&(t.inputs[e]=i,t.declaredInputs[e]=n.declaredInputs[e])}}function HN(t){let n=0,e=null;for(let i=t.length-1;i>=0;i--){let r=t[i];r.hostVars=n+=r.hostVars,r.hostAttrs=qo(r.hostAttrs,e=qo(e,r.hostAttrs))}}function vm(t){return t===Oi?{}:t===St?[]:t}function UN(t,n){let e=t.viewQuery;e?t.viewQuery=(i,r)=>{n(i,r),e(i,r)}:t.viewQuery=n}function zN(t,n){let e=t.contentQueries;e?t.contentQueries=(i,r,o)=>{n(i,r,o),e(i,r,o)}:t.contentQueries=n}function $N(t,n){let e=t.hostBindings;e?t.hostBindings=(i,r)=>{n(i,r),e(i,r)}:t.hostBindings=n}function rS(t,n,e,i,r,o,s,a){if(e.firstCreatePass){t.mergedAttrs=qo(t.mergedAttrs,t.attrs);let d=t.tView=wp(2,t,r,o,s,e.directiveRegistry,e.pipeRegistry,null,e.schemas,e.consts,null);e.queries!==null&&(e.queries.template(e,t),d.queries=e.queries.embeddedTView(t))}a&&(t.flags|=a),zo(t,!1);let l=WN(e,n,t,i);fd()&&Ep(e,n,l,t),Yo(l,n);let c=T0(l,n,l,t);n[i+Ze]=c,Mp(n,c),DN(c,t,n)}function GN(t,n,e,i,r,o,s,a,l,c,d){let f=e+Ze,h;return n.firstCreatePass?(h=es(n,f,4,s||null,a||null),Zh()&&B0(n,t,h,jt(n.consts,c),v0),Sy(n,h)):h=n.data[f],rS(h,t,n,e,i,r,o,l),Sa(h)&&Tp(n,t,h),c!=null&&qd(t,h,d),h}function Ko(t,n,e,i,r,o,s,a,l,c,d){let f=e+Ze,h;if(n.firstCreatePass){if(h=es(n,f,4,s||null,a||null),c!=null){let p=jt(n.consts,c);h.localNames=[];for(let S=0;S<p.length;S+=2)h.localNames.push(p[S],-1)}}else h=n.data[f];return rS(h,t,n,e,i,r,o,l),c!=null&&qd(t,h,d),h}function Dt(t,n,e,i,r,o,s,a){let l=se(),c=He(),d=jt(c.consts,o);return GN(l,c,t,n,e,i,r,d,void 0,s,a),Dt}function Kd(t,n,e,i,r,o,s,a){let l=se(),c=He(),d=jt(c.consts,o);return Ko(l,c,t,n,e,i,r,d,void 0,s,a),Kd}var WN=qN;function qN(t,n,e,i){return hd(!0),n[Fe].createComment("")}var Qd=(()=>{class t{log(e){console.log(e)}warn(e){console.warn(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=G({token:t,factory:t.\u0275fac,providedIn:"platform"})}return t})();var jp=new b("");var Ha=new b("");function oS(){Kf(()=>{let t="";throw new w(600,t)})}var YN=10;var Ft=(()=>{class t{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=u(Xt);afterRenderManager=u(Ud);zonelessEnabled=u(xa);rootEffectScheduler=u(pd);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new x;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=u(oi);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(oe(e=>!e))}constructor(){u($n,{optional:!0})}whenStable(){let e;return new Promise(i=>{e=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{e.unsubscribe()})}_injector=u(Me);_rendererFactory=null;get injector(){return this._injector}bootstrap(e,i){return this.bootstrapImpl(e,i)}bootstrapImpl(e,i,r=R.NULL){return this._injector.get(k).run(()=>{if(we(_e.BootstrapComponentStart),!this._injector.get(Vp).done){let H="";throw new w(405,H)}let a=Ri(e),l=this._injector.get(zn),c=new Xo(a,l);this.componentTypes.push(e);let{hostElement:d,directives:f,bindings:h}=ZN(i),p=d||c.selector,S=c.create(r,[],p,l.injector,f,h),E=S.location.nativeElement,$=S.injector.get(jp,null);return $?.registerApplication(E),S.onDestroy(()=>{this.detachView(S.hostView),Ma(this.components,S),$?.unregisterApplication(E)}),this._loadComponent(S),we(_e.BootstrapComponentEnd,S),S})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){we(_e.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(Hd.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw we(_e.ChangeDetectionEnd),new w(101,!1);let e=Q(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,Q(e),this.afterTick.next(),we(_e.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(Qe,null,{optional:!0}));let e=0;for(;this.dirtyFlags!==0&&e++<YN;){we(_e.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{we(_e.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let e=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!Ca(r))continue;let o=i&&!this.zonelessEnabled?0:1;x0(r,o),e=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}e||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:e})=>Ca(e))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(e){let i=e;this._views.push(i),i.attachToAppRef(this)}detachView(e){let i=e;Ma(this._views,i),i.detachFromAppRef()}_loadComponent(e){this.attachView(e.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(e),this._injector.get(Ha,[]).forEach(r=>r(e))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(e=>e()),this._views.slice().forEach(e=>e.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(e){return this._destroyListeners.push(e),()=>Ma(this._destroyListeners,e)}destroy(){if(this._destroyed)throw new w(406,!1);let e=this._injector;e.destroy&&!e.destroyed&&e.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();function ZN(t){return t===void 0||typeof t=="string"||t instanceof Element?{hostElement:t}:t}function Ma(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function re(t,n,e,i){let r=se(),o=Ur();if(Gi(r,o,n)){let s=He(),a=dd();_T(a,r,t,n,e,i)}return re}var Xm=class{destroy(n){}updateValue(n,e){}swap(n,e){let i=Math.min(n,e),r=Math.max(n,e),o=this.detach(r);if(r-i>1){let s=this.detach(i);this.attach(i,o),this.attach(r,s)}else this.attach(i,o)}move(n,e){this.attach(e,this.detach(n))}};function bm(t,n,e,i,r){return t===e&&Object.is(n,i)?1:Object.is(r(t,n),r(e,i))?-1:0}function XN(t,n,e,i){let r,o,s=0,a=t.length-1,l=void 0;if(Array.isArray(n)){Q(i);let c=n.length-1;for(Q(null);s<=a&&s<=c;){let d=t.at(s),f=n[s],h=bm(s,d,s,f,e);if(h!==0){h<0&&t.updateValue(s,f),s++;continue}let p=t.at(a),S=n[c],E=bm(a,p,c,S,e);if(E!==0){E<0&&t.updateValue(a,S),a--,c--;continue}let $=e(s,d),H=e(a,p),Se=e(s,f);if(Object.is(Se,H)){let Ge=e(c,S);Object.is(Ge,$)?(t.swap(s,a),t.updateValue(a,S),c--,a--):t.move(a,s),t.updateValue(s,f),s++;continue}if(r??=new Fd,o??=dy(t,s,a,e),Km(t,r,s,Se))t.updateValue(s,f),s++,a++;else if(o.has(Se))r.set($,t.detach(s)),a--;else{let Ge=t.create(s,n[s]);t.attach(s,Ge),s++,a++}}for(;s<=c;)cy(t,r,e,s,n[s]),s++}else if(n!=null){Q(i);let c=n[Symbol.iterator]();Q(null);let d=c.next();for(;!d.done&&s<=a;){let f=t.at(s),h=d.value,p=bm(s,f,s,h,e);if(p!==0)p<0&&t.updateValue(s,h),s++,d=c.next();else{r??=new Fd,o??=dy(t,s,a,e);let S=e(s,h);if(Km(t,r,s,S))t.updateValue(s,h),s++,a++,d=c.next();else if(!o.has(S))t.attach(s,t.create(s,h)),s++,a++,d=c.next();else{let E=e(s,f);r.set(E,t.detach(s)),a--}}}for(;!d.done;)cy(t,r,e,t.length,d.value),d=c.next()}for(;s<=a;)t.destroy(t.detach(a--));r?.forEach(c=>{t.destroy(c)})}function Km(t,n,e,i){return n!==void 0&&n.has(i)?(t.attach(e,n.get(i)),n.delete(i),!0):!1}function cy(t,n,e,i,r){if(Km(t,n,i,e(i,r)))t.updateValue(i,r);else{let o=t.create(i,r);t.attach(i,o)}}function dy(t,n,e,i){let r=new Set;for(let o=n;o<=e;o++)r.add(i(o,t.at(o)));return r}var Fd=class{kvMap=new Map;_vMap=void 0;has(n){return this.kvMap.has(n)}delete(n){if(!this.has(n))return!1;let e=this.kvMap.get(n);return this._vMap!==void 0&&this._vMap.has(e)?(this.kvMap.set(n,this._vMap.get(e)),this._vMap.delete(e)):this.kvMap.delete(n),!0}get(n){return this.kvMap.get(n)}set(n,e){if(this.kvMap.has(n)){let i=this.kvMap.get(n);this._vMap===void 0&&(this._vMap=new Map);let r=this._vMap;for(;r.has(i);)i=r.get(i);r.set(i,e)}else this.kvMap.set(n,e)}forEach(n){for(let[e,i]of this.kvMap)if(n(i,e),this._vMap!==void 0){let r=this._vMap;for(;r.has(i);)i=r.get(i),n(i,e)}}};function P(t,n,e,i,r,o,s,a){$i("NgControlFlow");let l=se(),c=He(),d=jt(c.consts,o);return Ko(l,c,t,n,e,i,r,d,256,s,a),Hp}function Hp(t,n,e,i,r,o,s,a){$i("NgControlFlow");let l=se(),c=He(),d=jt(c.consts,o);return Ko(l,c,t,n,e,i,r,d,512,s,a),Hp}function F(t,n){$i("NgControlFlow");let e=se(),i=Ur(),r=e[i]!==Qt?e[i]:-1,o=r!==-1?Ld(e,Ze+r):void 0,s=0;if(Gi(e,i,t)){let a=Q(null);try{if(o!==void 0&&k0(o,s),t!==-1){let l=Ze+t,c=Ld(e,l),d=tp(e[Z],l),f=R0(c,d,e),h=Va(e,d,n,{dehydratedView:f});Ba(c,h,s,Zo(d,f))}}finally{Q(a)}}else if(o!==void 0){let a=N0(o,s);a!==void 0&&(a[Ye]=n)}}var Qm=class{lContainer;$implicit;$index;constructor(n,e,i){this.lContainer=n,this.$implicit=e,this.$index=i}get $count(){return this.lContainer.length-je}};function Up(t){return t}function Jd(t,n){return n}var Jm=class{hasEmptyBlock;trackByFn;liveCollection;constructor(n,e,i){this.hasEmptyBlock=n,this.trackByFn=e,this.liveCollection=i}};function Jt(t,n,e,i,r,o,s,a,l,c,d,f,h){$i("NgControlFlow");let p=se(),S=He(),E=l!==void 0,$=se(),H=a?s.bind($[kt][Ye]):s,Se=new Jm(E,H);$[Ze+t]=Se,Ko(p,S,t+1,n,e,i,r,jt(S.consts,o),256),E&&Ko(p,S,t+2,l,c,d,f,jt(S.consts,h),512)}var ep=class extends Xm{lContainer;hostLView;templateTNode;operationsCounter=void 0;needsIndexUpdate=!1;constructor(n,e,i){super(),this.lContainer=n,this.hostLView=e,this.templateTNode=i}get length(){return this.lContainer.length-je}at(n){return this.getLView(n)[Ye].$implicit}attach(n,e){let i=e[Pr];this.needsIndexUpdate||=n!==this.length,Ba(this.lContainer,e,n,Zo(this.templateTNode,i)),KN(this.lContainer,n)}detach(n){return this.needsIndexUpdate||=n!==this.length-1,QN(this.lContainer,n),JN(this.lContainer,n)}create(n,e){let i=Td(this.lContainer,this.templateTNode.tView.ssrId);return Va(this.hostLView,this.templateTNode,new Qm(this.lContainer,e,n),{dehydratedView:i})}destroy(n){$d(n[Z],n)}updateValue(n,e){this.getLView(n)[Ye].$implicit=e}reset(){this.needsIndexUpdate=!1}updateIndexes(){if(this.needsIndexUpdate)for(let n=0;n<this.length;n++)this.getLView(n)[Ye].$index=n}getLView(n){return ek(this.lContainer,n)}};function en(t){let n=Q(null),e=ri();try{let i=se(),r=i[Z],o=i[e],s=e+1,a=Ld(i,s);if(o.liveCollection===void 0){let c=tp(r,s);o.liveCollection=new ep(a,i,c)}else o.liveCollection.reset();let l=o.liveCollection;if(XN(l,t,o.trackByFn,n),l.updateIndexes(),o.hasEmptyBlock){let c=Ur(),d=l.length===0;if(Gi(i,c,d)){let f=e+2,h=Ld(i,f);if(d){let p=tp(r,f),S=R0(h,p,i),E=Va(i,p,void 0,{dehydratedView:S});Ba(h,E,0,Zo(p,S))}else r.firstUpdatePass&&zT(h),k0(h,0)}}}finally{Q(n)}}function Ld(t,n){return t[n]}function KN(t,n){if(t.length<=je)return;let e=je+n,i=t[e],r=i?i[jn]:void 0;if(i&&r&&r.detachedLeaveAnimationFns&&r.detachedLeaveAnimationFns.length>0){let o=i[Ln];zM(o,r),Hi.delete(i[Bn]),r.detachedLeaveAnimationFns=void 0}}function QN(t,n){if(t.length<=je)return;let e=je+n,i=t[e],r=i?i[jn]:void 0;r&&r.leave&&r.leave.size>0&&(r.detachedLeaveAnimationFns=[])}function JN(t,n){return Aa(t,n)}function ek(t,n){return N0(t,n)}function tp(t,n){return nd(t,n)}function L(t,n,e){let i=se(),r=Ur();if(Gi(i,r,n)){let o=He(),s=dd();mT(s,i,t,n,i[Fe],e)}return L}function np(t,n,e,i,r){Np(n,t,e,r?"class":"style",i)}function m(t,n,e,i){let r=se(),o=r[Z],s=t+Ze,a=o.firstCreatePass?H0(s,r,2,n,v0,Zh(),e,i):o.data[s];if(ii(a)){let l=r[pn].tracingService;if(l&&l.componentCreate){let c=o.data[a.directiveStart+a.componentOffset];return l.componentCreate(F0(c),()=>(uy(t,n,r,a,i),m))}}return uy(t,n,r,a,i),m}function uy(t,n,e,i,r){if(b0(i,e,t,n,sS),Sa(i)){let o=e[Z];Tp(o,e,i),Wy(o,i,e)}r!=null&&qd(e,i)}function g(){let t=He(),n=vt(),e=_0(n);return t.firstCreatePass&&U0(t,e),Kh(e)&&Qh(),Yh(),e.classesWithoutHost!=null&&OI(e)&&np(t,e,se(),e.classesWithoutHost,!0),e.stylesWithoutHost!=null&&PI(e)&&np(t,e,se(),e.stylesWithoutHost,!1),g}function ie(t,n,e,i){return m(t,n,e,i),g(),ie}function lt(t,n,e,i){let r=se(),o=r[Z],s=t+Ze,a=o.firstCreatePass?aN(s,o,2,n,e,i):o.data[s];return b0(a,r,t,n,sS),i!=null&&qd(r,a),lt}function _t(){let t=vt(),n=_0(t);return Kh(n)&&Qh(),Yh(),_t}function tn(t,n,e,i){return lt(t,n,e,i),_t(),tn}var sS=(t,n,e,i,r)=>(hd(!0),Jy(n[Fe],i,sm()));function Le(){return se()}function Et(t,n,e){let i=se(),r=Ur();if(Gi(i,r,n)){let o=He(),s=dd();g0(s,i,t,n,i[Fe],e)}return Et}var Ua="en-US";var tk=Ua;function aS(t){typeof t=="string"&&(tk=t.toLowerCase().replace(/_/g,"-"))}function D(t,n,e){let i=se(),r=He(),o=vt();return nk(r,i,i[Fe],o,t,n,e),D}function ts(t,n,e){let i=se(),r=He(),o=vt();return(o.type&3||e)&&L0(o,r,i,e,i[Fe],t,n,Ed(o,i,n)),ts}function nk(t,n,e,i,r,o,s){let a=!0,l=null;if((i.type&3||s)&&(l??=Ed(i,n,o),L0(i,t,n,s,e,r,o,l)&&(a=!1)),a){let c=i.outputs?.[r],d=i.hostDirectiveOutputs?.[r];if(d&&d.length)for(let f=0;f<d.length;f+=2){let h=d[f],p=d[f+1];l??=Ed(i,n,o),J_(i,n,h,p,r,l)}if(c&&c.length)for(let f of c)l??=Ed(i,n,o),J_(i,n,f,r,r,l)}}function C(t=1){return C_(t)}function ik(t,n){let e=null,i=NM(t);for(let r=0;r<n.length;r++){let o=n[r];if(o==="*"){e=r;continue}if(i===null?r0(t,o,!0):RM(i,o))return r}return e}function Pe(t){let n=se()[kt][pt];if(!n.projection){let e=t?t.length:1,i=n.projection=qb(e,null),r=i.slice(),o=n.child;for(;o!==null;){if(o.type!==128){let s=t?ik(o,t):0;s!==null&&(r[s]?r[s].projectionNext=o:i[s]=o,r[s]=o)}o=o.next}}}function me(t,n=0,e,i,r,o){let s=se(),a=He(),l=i?t+1:null;l!==null&&Ko(s,a,l,i,r,o,null,e);let c=es(a,Ze+t,16,null,e||null);c.projection===null&&(c.projection=n),tm();let f=!s[Pr]||Xh();s[kt][pt].projection[c.projection]===null&&l!==null?rk(s,a,l):f&&!Bd(c)&&rT(a,s,c)}function rk(t,n,e){let i=Ze+e,r=n.data[i],o=t[i],s=Td(o,r.tView.ssrId),a=Va(t,r,void 0,{dehydratedView:s});Ba(o,a,0,Zo(r,s))}function Gn(t,n,e,i){return Z0(t,n,e,i),Gn}function Ue(t,n,e){return Y0(t,n,e),Ue}function ce(t){let n=se(),e=He(),i=ad();Da(i+1);let r=Op(e,i);if(t.dirty&&r_(n)===((r.metadata.flags&2)===2)){if(r.matches===null)t.reset([]);else{let o=K0(n,i);t.reset(o,Ry),t.notifyOnChanges()}return!0}return!1}function de(){return Rp(se(),ad())}function eu(t,n,e,i,r){return J0(n,Z0(t,e,i,r)),eu}function Wi(t,n,e,i){return J0(t,Y0(n,e,i)),Wi}function qi(t=1){Da(ad()+t)}function ht(t){let n=f_();return i_(n,Ze+t)}function _d(t,n){return t<<17|n<<2}function Wr(t){return t>>17&32767}function ok(t){return(t&2)==2}function sk(t,n){return t&131071|n<<17}function ip(t){return t|2}function Qo(t){return(t&131068)>>2}function _m(t,n){return t&-131069|n<<2}function ak(t){return(t&1)===1}function rp(t){return t|1}function lk(t,n,e,i,r,o){let s=o?n.classBindings:n.styleBindings,a=Wr(s),l=Qo(s);t[i]=e;let c=!1,d;if(Array.isArray(e)){let f=e;d=f[1],(d===null||Vo(f,d)>0)&&(c=!0)}else d=e;if(r)if(l!==0){let h=Wr(t[a+1]);t[i+1]=_d(h,a),h!==0&&(t[h+1]=_m(t[h+1],i)),t[a+1]=sk(t[a+1],i)}else t[i+1]=_d(a,0),a!==0&&(t[a+1]=_m(t[a+1],i)),a=i;else t[i+1]=_d(l,0),a===0?a=i:t[l+1]=_m(t[l+1],i),l=i;c&&(t[i+1]=ip(t[i+1])),fy(t,d,i,!0),fy(t,d,i,!1),ck(n,d,t,i,o),s=_d(a,l),o?n.classBindings=s:n.styleBindings=s}function ck(t,n,e,i,r){let o=r?t.residualClasses:t.residualStyles;o!=null&&typeof n=="string"&&Vo(o,n)>=0&&(e[i+1]=rp(e[i+1]))}function fy(t,n,e,i){let r=t[e+1],o=n===null,s=i?Wr(r):Qo(r),a=!1;for(;s!==0&&(a===!1||o);){let l=t[s],c=t[s+1];dk(l,n)&&(a=!0,t[s+1]=i?rp(c):ip(c)),s=i?Wr(c):Qo(c)}a&&(t[e+1]=i?ip(r):rp(r))}function dk(t,n){return t===null||n==null||(Array.isArray(t)?t[1]:t)===n?!0:Array.isArray(t)&&typeof n=="string"?Vo(t,n)>=0:!1}var _n={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function uk(t){return t.substring(_n.key,_n.keyEnd)}function fk(t){return hk(t),lS(t,cS(t,0,_n.textEnd))}function lS(t,n){let e=_n.textEnd;return e===n?-1:(n=_n.keyEnd=mk(t,_n.key=n,e),cS(t,n,e))}function hk(t){_n.key=0,_n.keyEnd=0,_n.value=0,_n.valueEnd=0,_n.textEnd=t.length}function cS(t,n,e){for(;n<e&&t.charCodeAt(n)<=32;)n++;return n}function mk(t,n,e){for(;n<e&&t.charCodeAt(n)>32;)n++;return n}function Ut(t,n,e){return dS(t,n,e,!1),Ut}function B(t,n){return dS(t,n,null,!0),B}function xt(t){gk(Ck,pk,t,!0)}function pk(t,n){for(let e=fk(n);e>=0;e=lS(n,e))Jc(t,uk(n),!0)}function dS(t,n,e,i){let r=se(),o=He(),s=im(2);if(o.firstUpdatePass&&fS(o,t,s,i),n!==Qt&&Gi(r,s,n)){let a=o.data[ri()];hS(o,a,r,r[Fe],t,r[s+1]=Ek(n,e),i,s)}}function gk(t,n,e,i){let r=He(),o=im(2);r.firstUpdatePass&&fS(r,null,o,i);let s=se();if(e!==Qt&&Gi(s,o,e)){let a=r.data[ri()];if(mS(a,i)&&!uS(r,o)){let l=i?a.classesWithoutHost:a.stylesWithoutHost;l!==null&&(e=Wc(l,e||"")),np(r,a,s,e,i)}else Dk(r,a,s,s[Fe],s[o+1],s[o+1]=Sk(t,n,e),i,o)}}function uS(t,n){return n>=t.expandoStartIndex}function fS(t,n,e,i){let r=t.data;if(r[e+1]===null){let o=r[ri()],s=uS(t,e);mS(o,i)&&n===null&&!s&&(n=!1),n=vk(r,o,n,i),lk(r,o,n,e,s,i)}}function vk(t,n,e,i){let r=b_(t),o=i?n.residualClasses:n.residualStyles;if(r===null)(i?n.classBindings:n.styleBindings)===0&&(e=ym(null,t,n,e,i),e=Oa(e,n.attrs,i),o=null);else{let s=n.directiveStylingLast;if(s===-1||t[s]!==r)if(e=ym(r,t,n,e,i),o===null){let l=bk(t,n,i);l!==void 0&&Array.isArray(l)&&(l=ym(null,t,n,l[1],i),l=Oa(l,n.attrs,i),_k(t,n,i,l))}else o=yk(t,n,i)}return o!==void 0&&(i?n.residualClasses=o:n.residualStyles=o),e}function bk(t,n,e){let i=e?n.classBindings:n.styleBindings;if(Qo(i)!==0)return t[Wr(i)]}function _k(t,n,e,i){let r=e?n.classBindings:n.styleBindings;t[Wr(r)]=i}function yk(t,n,e){let i,r=n.directiveEnd;for(let o=1+n.directiveStylingLast;o<r;o++){let s=t[o].hostAttrs;i=Oa(i,s,e)}return Oa(i,n.attrs,e)}function ym(t,n,e,i,r){let o=null,s=e.directiveEnd,a=e.directiveStylingLast;for(a===-1?a=e.directiveStart:a++;a<s&&(o=n[a],i=Oa(i,o.hostAttrs,r),o!==t);)a++;return t!==null&&(e.directiveStylingLast=a),i}function Oa(t,n,e){let i=e?1:2,r=-1;if(n!==null)for(let o=0;o<n.length;o++){let s=n[o];typeof s=="number"?r=s:r===i&&(Array.isArray(t)||(t=t===void 0?[]:["",t]),Jc(t,s,e?!0:n[++o]))}return t===void 0?null:t}function Sk(t,n,e){if(e==null||e==="")return St;let i=[],r=Sn(e);if(Array.isArray(r))for(let o=0;o<r.length;o++)t(i,r[o],!0);else if(r instanceof Set)for(let o of r)t(i,o,!0);else if(typeof r=="object")for(let o in r)Object.hasOwn(r,o)&&t(i,o,r[o]);else typeof r=="string"&&n(i,r);return i}function Ck(t,n,e){let i=String(n);i!==""&&!i.includes(" ")&&Jc(t,i,e)}function Dk(t,n,e,i,r,o,s,a){r===Qt&&(r=St);let l=0,c=0,d=0<r.length?r[0]:null,f=0<o.length?o[0]:null;for(;d!==null||f!==null;){let h=l<r.length?r[l+1]:void 0,p=c<o.length?o[c+1]:void 0,S=null,E;d===f?(l+=2,c+=2,h!==p&&(S=f,E=p)):f===null||d!==null&&d<f?(l+=2,S=d):(c+=2,S=f,E=p),S!==null&&hS(t,n,e,i,S,E,s,a),d=l<r.length?r[l]:null,f=c<o.length?o[c]:null}}function hS(t,n,e,i,r,o,s,a){if(!(n.type&3))return;let l=t.data,c=l[a+1],d=ak(c)?hy(l,n,e,r,Qo(c),s):void 0;if(!Vd(d)){Vd(o)||ok(c)&&(o=hy(l,null,e,r,a,s));let f=zh(ri(),e);sT(i,s,f,r,o)}}function hy(t,n,e,i,r,o){let s=n===null,a;for(;r>0;){let l=t[r],c=Array.isArray(l),d=c?l[1]:l,f=d===null,h=e[r+1];h===Qt&&(h=f?St:void 0);let p=f?ed(h,i):d===i?h:void 0;if(c&&!Vd(p)&&(p=ed(l,i)),Vd(p)&&(a=p,s))return a;let S=t[r+1];r=s?Wr(S):Qo(S)}if(n!==null){let l=o?n.residualClasses:n.residualStyles;l!=null&&(a=ed(l,i))}return a}function Vd(t){return t!==void 0}function Ek(t,n){return t==null||t===""||(typeof n=="string"?t=Sn(t)+n:typeof t=="object"&&(t=ha(Sn(t)))),t}function mS(t,n){return(t.flags&(n?8:16))!==0}function y(t,n=""){let e=se(),i=He(),r=t+Ze,o=i.firstCreatePass?es(i,r,1,n,null):i.data[r],s=xk(i,e,o,n);e[r]=s,fd()&&Ep(i,e,s,o),zo(o,!1)}var xk=(t,n,e,i)=>(hd(!0),_M(n[Fe],i));function wk(t,n,e,i=""){return Gi(t,Ur(),e)?n+Xc(e)+i:Qt}function ye(t){return At("",t),ye}function At(t,n,e){let i=se(),r=wk(i,t,n,e);return r!==Qt&&Ik(i,ri(),r),At}function Ik(t,n,e){let i=zh(n,t);yM(t[Fe],i,e)}function my(t,n,e){let i=He();i.firstCreatePass&&pS(n,i.data,i.blueprint,gn(t),e)}function pS(t,n,e,i,r){if(t=it(t),Array.isArray(t))for(let o=0;o<t.length;o++)pS(t[o],n,e,i,r);else{let o=He(),s=se(),a=vt(),l=kr(t)?t:it(t.provide),c=Lh(t),d=a.providerIndexes&1048575,f=a.directiveStart,h=a.providerIndexes>>20;if(kr(t)||!t.multi){let p=new Gr(c,r,ft,null),S=Cm(l,n,r?d:d+h,f);S===-1?(xm(Md(a,s),o,l),Sm(o,t,n.length),n.push(l),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),e.push(p),s.push(p)):(e[S]=p,s[S]=p)}else{let p=Cm(l,n,d+h,f),S=Cm(l,n,d,d+h),E=p>=0&&e[p],$=S>=0&&e[S];if(r&&!$||!r&&!E){xm(Md(a,s),o,l);let H=Nk(r?Tk:Mk,e.length,r,i,c,t);!r&&$&&(e[S].providerFactory=H),Sm(o,t,n.length,0),n.push(l),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),e.push(H),s.push(H)}else{let H=gS(e[r?S:p],c,!r&&i);Sm(o,t,p>-1?p:S,H)}!r&&i&&$&&e[S].componentProviders++}}}function Sm(t,n,e,i){let r=kr(n),o=Jb(n);if(r||o){let l=(o?it(n.useClass):n).prototype.ngOnDestroy;if(l){let c=t.destroyHooks||(t.destroyHooks=[]);if(!r&&n.multi){let d=c.indexOf(e);d===-1?c.push(e,[i,l]):c[d+1].push(i,l)}else c.push(e,l)}}}function gS(t,n,e){return e&&t.componentProviders++,t.multi.push(n)-1}function Cm(t,n,e,i){for(let r=e;r<i;r++)if(n[r]===t)return r;return-1}function Mk(t,n,e,i,r){return op(this.multi,[])}function Tk(t,n,e,i,r){let o=this.multi,s;if(this.providerFactory){let a=this.providerFactory.componentProviders,l=Ta(i,i[Z],this.providerFactory.index,r);s=l.slice(0,a),op(o,s);for(let c=a;c<l.length;c++)s.push(l[c])}else s=[],op(o,s);return s}function op(t,n){for(let e=0;e<t.length;e++){let i=t[e];n.push(i())}return n}function Nk(t,n,e,i,r,o){let s=new Gr(t,e,ft,null);return s.multi=[],s.index=n,s.componentProviders=0,gS(s,r,i&&!e),s}function Be(t,n){return e=>{e.providersResolver=(i,r)=>my(i,r?r(t):t,!1),n&&(e.viewProvidersResolver=(i,r)=>my(i,r?r(n):n,!0))}}function zp(t,n){let e=h_()+t,i=se();return i[e]===Qt?YT(i,e,n()):ZT(i,e)}function $p(t,n){return Yd(t,n)}var vS=(()=>{class t{applicationErrorHandler=u(Xt);appRef=u(Ft);taskService=u(oi);ngZone=u(k);zonelessEnabled=u(xa);tracing=u($n,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new pe;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(ua):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(u(fm,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let e=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(e);return}this.switchToMicrotaskScheduler(),this.taskService.remove(e)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let e=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(e)})})}notify(e){if(!this.zonelessEnabled&&e===5)return;switch(e){case 0:case 2:{this.appRef.dirtyFlags|=2;break}case 3:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?w_:lm;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(ua+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let e=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(e),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let e=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();function bS(){return[{provide:Pn,useExisting:vS},{provide:k,useClass:fa},{provide:xa,useValue:!0}]}var Gp=(()=>{class t{compileModuleSync(e){return new Pd(e)}compileModuleAsync(e){return Promise.resolve(this.compileModuleSync(e))}clearCache(){}clearCacheFor(e){}getModuleId(e){}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();function kk(){return typeof $localize<"u"&&$localize.locale||Ua}var tu=new b("",{factory:()=>u(tu,{optional:!0,skipSelf:!0})||kk()});function K(t,n){return Xs(t,n?.equal)}function Ne(t){return sb(t)}var _S=class t extends Error{_brand;constructor(n){super(n)}static IDLE=new t("IDLE");static LOADING=new t("LOADING")};function wS(t,n){let e=Object.create(vy);e.value=t,e.transformFn=n?.transform;function i(){if(br(e),e.value===sp){let r=null;throw new w(-950,r)}return e.value}return i[et]=e,i}var Cn=class{attributeName;constructor(n){this.attributeName=n}__NG_ELEMENT_ID__=()=>lp(this.attributeName);toString(){return`HostAttributeToken ${this.attributeName}`}};function Zp(t){return Zk(t)?t.default:t}function Zk(t){return t&&typeof t=="object"&&"default"in t}function yS(t,n){return wS(t,n)}function Xk(t){return wS(sp,t)}var zt=(yS.required=Xk,yS);function SS(t,n){return Pp(n)}function Kk(t,n){return Fp(n)}var ui=(SS.required=Kk,SS);function CS(t,n){return Pp(n)}function Qk(t,n){return Fp(n)}var IS=(CS.required=Qk,CS);var Jk=1e4;var Kq=Jk-1e3;var Oe=(()=>{class t{static __NG_ELEMENT_ID__=eA}return t})();function eA(t){return tA(vt(),se(),(t&16)===16)}function tA(t,n,e){if(ii(t)&&!e){let i=Zt(t.index,n);return new Ui(i,i)}else if(t.type&175){let i=n[kt];return new Ui(i,n)}return null}var qp=new b(""),nA=new b("");function za(t){return!t.moduleRef}function iA(t){let n=za(t)?t.r3Injector:t.moduleRef.injector,e=n.get(k);return e.run(()=>{za(t)?t.r3Injector.resolveInjectorInitializers():t.moduleRef.resolveInjectorInitializers();let i=n.get(Xt),r;if(e.runOutsideAngular(()=>{r=e.onError.subscribe({next:i})}),za(t)){let o=()=>n.destroy(),s=t.platformInjector.get(qp);s.add(o),n.onDestroy(()=>{r.unsubscribe(),s.delete(o)})}else{let o=()=>t.moduleRef.destroy(),s=t.platformInjector.get(qp);s.add(o),t.moduleRef.onDestroy(()=>{Ma(t.allPlatformModules,t.moduleRef),r.unsubscribe(),s.delete(o)})}return oA(i,e,()=>{let o=n.get(oi),s=o.add(),a=n.get(Vp);return a.runInitializers(),a.donePromise.then(()=>{let l=n.get(tu,Ua);if(aS(l||Ua),!n.get(nA,!0))return za(t)?n.get(Ft):(t.allPlatformModules.push(t.moduleRef),t.moduleRef);if(za(t)){let d=n.get(Ft);return t.rootComponent!==void 0&&d.bootstrap(t.rootComponent),d}else return rA?.(t.moduleRef,t.allPlatformModules),t.moduleRef}).finally(()=>{o.remove(s)})})})}var rA;function oA(t,n,e){try{let i=e();return di(i)?i.catch(r=>{throw n.runOutsideAngular(()=>t(r)),r}):i}catch(i){throw n.runOutsideAngular(()=>t(i)),i}}var nu=null;function sA(t=[],n){return R.create({name:n,providers:[{provide:_a,useValue:"platform"},{provide:qp,useValue:new Set([()=>nu=null])},...t]})}function aA(t=[]){if(nu)return nu;let n=sA(t);return nu=n,oS(),lA(n),n}function lA(t){let n=t.get(md,null);ot(t,()=>{n?.forEach(e=>e())})}function MS(t){let{rootComponent:n,appProviders:e,platformProviders:i,platformRef:r}=t;we(_e.BootstrapApplicationStart);try{let o=r?.injector??aA(i),s=[bS(),M_,...e||[]],a=new Ra({providers:s,parent:o,debugName:"",runEnvironmentInitializers:!1});return iA({r3Injector:a.injector,platformInjector:o,rootComponent:n})}catch(o){return Promise.reject(o)}finally{we(_e.BootstrapApplicationEnd)}}function V(t){return typeof t=="boolean"?t:t!=null&&t!=="false"}function Xr(t,n=NaN){return!isNaN(parseFloat(t))&&!isNaN(Number(t))?Number(t):n}var Wp=Symbol("NOT_SET"),TS=new Set,cA=N(v({},Mo),{kind:"afterRenderEffectPhase",consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,value:Wp,cleanup:null,consumerMarkedDirty(){if(this.sequence.impl.executing){if(this.sequence.lastPhase===null||this.sequence.lastPhase<this.phase)return;this.sequence.erroredOrDestroyed=!0}this.sequence.scheduler.notify(7)},phaseFn(t){if(this.sequence.lastPhase=this.phase,!this.dirty)return this.signal;if(this.dirty=!1,this.value!==Wp&&!wo(this))return this.signal;try{for(let r of this.cleanup??TS)r()}finally{this.cleanup?.clear()}let n=[];t!==void 0&&n.push(t),n.push(this.registerCleanupFn);let e=Ti(this),i;try{i=this.userFn.apply(null,n)}finally{_r(this,e)}return(this.value===Wp||!this.equal(this.value,i))&&(this.value=i,this.version++),this.signal}}),Yp=class extends Na{scheduler;lastPhase=null;nodes=[void 0,void 0,void 0,void 0];onDestroyFns=null;constructor(n,e,i,r,o,s=null){super(n,[void 0,void 0,void 0,void 0],i,!1,o.get(qe),s),this.scheduler=r;for(let a of yp){let l=e[a];if(l===void 0)continue;let c=Object.create(cA);c.sequence=this,c.phase=a,c.userFn=l,c.dirty=!0,c.signal=()=>(br(c),c.value),c.signal[et]=c,c.registerCleanupFn=d=>(c.cleanup??=new Set).add(d),this.nodes[a]=c,this.hooks[a]=d=>c.phaseFn(d)}}afterRun(){super.afterRun(),this.lastPhase=null}destroy(){if(this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();super.destroy();for(let n of this.nodes)if(n)try{for(let e of n.cleanup??TS)e()}finally{Ni(n)}}};function Kr(t,n){let e=n?.injector??u(R),i=e.get(Pn),r=e.get(Ud),o=e.get($n,null,{optional:!0});r.impl??=e.get(Sp);let s=t;typeof s=="function"&&(s={mixedReadWrite:t});let a=e.get($o,null,{optional:!0}),l=new Yp(r.impl,[s.earlyRead,s.write,s.mixedReadWrite,s.read],a?.view,i,e,o?.snapshot(null));return r.impl.register(l),l}function iu(t,n){let e=Ri(t),i=n.elementInjector||Bo();return new Xo(e).create(i,n.projectableNodes,n.hostElement,n.environmentInjector,n.directives,n.bindings)}var NS=null;function nn(){return NS}function Xp(t){NS??=t}var $a=class{},ns=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=G({token:t,factory:()=>u(kS),providedIn:"platform"})}return t})();var kS=(()=>{class t extends ns{_location;_history;_doc=u(j);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return nn().getBaseHref(this._doc)}onPopState(e){let i=nn().getGlobalEventTarget(this._doc,"window");return i.addEventListener("popstate",e,!1),()=>i.removeEventListener("popstate",e)}onHashChange(e){let i=nn().getGlobalEventTarget(this._doc,"window");return i.addEventListener("hashchange",e,!1),()=>i.removeEventListener("hashchange",e)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(e){this._location.pathname=e}pushState(e,i,r){this._history.pushState(e,i,r)}replaceState(e,i,r){this._history.replaceState(e,i,r)}forward(){this._history.forward()}back(){this._history.back()}historyGo(e=0){this._history.go(e)}getState(){return this._history.state}static \u0275fac=function(i){return new(i||t)};static \u0275prov=G({token:t,factory:()=>new t,providedIn:"platform"})}return t})();function OS(t,n){return t?n?t.endsWith("/")?n.startsWith("/")?t+n.slice(1):t+n:n.startsWith("/")?t+n:`${t}/${n}`:t:n}function AS(t){let n=t.search(/#|\?|$/);return t[n-1]==="/"?t.slice(0,n-1)+t.slice(n):t}function Yi(t){return t&&t[0]!=="?"?`?${t}`:t}var ru=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=G({token:t,factory:()=>u(uA),providedIn:"root"})}return t})(),dA=new b(""),uA=(()=>{class t extends ru{_platformLocation;_baseHref;_removeListenerFns=[];constructor(e,i){super(),this._platformLocation=e,this._baseHref=i??this._platformLocation.getBaseHrefFromDOM()??u(j).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(e){this._removeListenerFns.push(this._platformLocation.onPopState(e),this._platformLocation.onHashChange(e))}getBaseHref(){return this._baseHref}prepareExternalUrl(e){return OS(this._baseHref,e)}path(e=!1){let i=this._platformLocation.pathname+Yi(this._platformLocation.search),r=this._platformLocation.hash;return r&&e?`${i}${r}`:i}pushState(e,i,r,o){let s=this.prepareExternalUrl(r+Yi(o));this._platformLocation.pushState(e,i,s)}replaceState(e,i,r,o){let s=this.prepareExternalUrl(r+Yi(o));this._platformLocation.replaceState(e,i,s)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(e=0){this._platformLocation.historyGo?.(e)}static \u0275fac=function(i){return new(i||t)(A(ns),A(dA,8))};static \u0275prov=G({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Zi=(()=>{class t{_subject=new x;_basePath;_locationStrategy;_urlChangeListeners=[];_urlChangeSubscription=null;constructor(e){this._locationStrategy=e;let i=this._locationStrategy.getBaseHref();this._basePath=mA(AS(RS(i))),this._locationStrategy.onPopState(r=>{let o={url:this.path(!0),pop:!0,state:r.state,type:r.type};r.hasUAVisualTransition&&(o.hasUAVisualTransition=!0),this._subject.next(o)})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(e=!1){return this.normalize(this._locationStrategy.path(e))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(e,i=""){return this.path()==this.normalize(e+Yi(i))}normalize(e){return t.stripTrailingSlash(hA(this._basePath,RS(e)))}prepareExternalUrl(e){return e&&e[0]!=="/"&&(e="/"+e),this._locationStrategy.prepareExternalUrl(e)}go(e,i="",r=null){this._locationStrategy.pushState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+Yi(i)),r)}replaceState(e,i="",r=null){this._locationStrategy.replaceState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+Yi(i)),r)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(e=0){this._locationStrategy.historyGo?.(e)}onUrlChange(e){return this._urlChangeListeners.push(e),this._urlChangeSubscription??=this.subscribe(i=>{this._notifyUrlChangeListeners(i.url,i.state)}),()=>{let i=this._urlChangeListeners.indexOf(e);this._urlChangeListeners.splice(i,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(e="",i){this._urlChangeListeners.forEach(r=>r(e,i))}subscribe(e,i,r){return this._subject.subscribe({next:e,error:i??void 0,complete:r??void 0})}static normalizeQueryParams=Yi;static joinWithSlash=OS;static stripTrailingSlash=AS;static \u0275fac=function(i){return new(i||t)(A(ru))};static \u0275prov=G({token:t,factory:()=>fA(),providedIn:"root"})}return t})();function fA(){return new Zi(A(ru))}function hA(t,n){if(!t||!n.startsWith(t))return n;let e=n.substring(t.length);return e===""||["/",";","?","#"].includes(e[0])?e:n}function RS(t){return t.replace(/\/index\.html$/,"")}function mA(t){if(new RegExp("^(https?:)?//").test(t)){let[,e]=t.split(/\/\/[^\/]+/);return e}return t}var Kp=(()=>{class t{_viewContainerRef;_viewRef=null;ngTemplateOutletContext=null;ngTemplateOutlet=null;ngTemplateOutletInjector=null;injector=u(R);constructor(e){this._viewContainerRef=e}ngOnChanges(e){if(this._shouldRecreateView(e)){let i=this._viewContainerRef;if(this._viewRef&&i.remove(i.indexOf(this._viewRef)),!this.ngTemplateOutlet){this._viewRef=null;return}let r=this._createContextForwardProxy();this._viewRef=i.createEmbeddedView(this.ngTemplateOutlet,r,{injector:this._getInjector()})}}_getInjector(){return this.ngTemplateOutletInjector==="outlet"?this.injector:this.ngTemplateOutletInjector??void 0}_shouldRecreateView(e){return!!e.ngTemplateOutlet||!!e.ngTemplateOutletInjector}_createContextForwardProxy(){return new Proxy({},{set:(e,i,r)=>this.ngTemplateOutletContext?Reflect.set(this.ngTemplateOutletContext,i,r):!1,get:(e,i,r)=>{if(this.ngTemplateOutletContext)return Reflect.get(this.ngTemplateOutletContext,i,r)}})}static \u0275fac=function(i){return new(i||t)(ft(bt))};static \u0275dir=z({type:t,selectors:[["","ngTemplateOutlet",""]],inputs:{ngTemplateOutletContext:"ngTemplateOutletContext",ngTemplateOutlet:"ngTemplateOutlet",ngTemplateOutletInjector:"ngTemplateOutletInjector"},features:[at]})}return t})();function Ga(t,n){n=encodeURIComponent(n);for(let e of t.split(";")){let i=e.indexOf("="),[r,o]=i==-1?[e,""]:[e.slice(0,i),e.slice(i+1)];if(r.trim()!==n)continue;let s=o;try{s=decodeURIComponent(o)}catch{}return s.length>1&&s[0]==='"'&&s[s.length-1]==='"'&&(s=s.slice(1,-1)),s}return null}var Qp="browser";function PS(t){return t===Qp}var Wa=class{_doc;constructor(n){this._doc=n}manager},ou=(()=>{class t extends Wa{constructor(e){super(e)}supports(e){return!0}addEventListener(e,i,r,o){return e.addEventListener(i,r,o),()=>this.removeEventListener(e,i,r,o)}removeEventListener(e,i,r,o){return e.removeEventListener(i,r,o)}static \u0275fac=function(i){return new(i||t)(A(j))};static \u0275prov=G({token:t,factory:t.\u0275fac})}return t})(),lu=new b(""),ng=(()=>{class t{_zone;_plugins;_eventNameToPlugin=new Map;constructor(e,i){this._zone=i,e.forEach(s=>{s.manager=this});let r=e.filter(s=>!(s instanceof ou));this._plugins=r.slice().reverse();let o=e.find(s=>s instanceof ou);o&&this._plugins.push(o)}addEventListener(e,i,r,o){return this._findPluginFor(i).addEventListener(e,i,r,o)}getZone(){return this._zone}_findPluginFor(e){let i=this._eventNameToPlugin.get(e);if(i)return i;if(i=this._plugins.find(o=>o.supports(e)),!i)throw new w(-5101,!1);return this._eventNameToPlugin.set(e,i),i}static \u0275fac=function(i){return new(i||t)(A(lu),A(k))};static \u0275prov=G({token:t,factory:t.\u0275fac})}return t})(),Jp="ng-app-id";function FS(t){for(let n of t)n.remove()}function LS(t,n){let e=n.createElement("style");return e.textContent=t,e}function _A(t,n,e,i){let r=t.head?.querySelectorAll(`style[${Jp}="${n}"],link[${Jp}="${n}"]`);if(!r||r.length===0)return!1;for(let o of r)o.removeAttribute(Jp),o instanceof HTMLLinkElement?i.set(o.href.slice(o.href.lastIndexOf("/")+1),{usage:0,elements:[o]}):o.textContent&&e.set(o.textContent,{usage:0,elements:[o]});return!0}function tg(t,n){let e=n.createElement("link");return e.setAttribute("rel","stylesheet"),e.setAttribute("href",t),e}var ig=(()=>{class t{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(e,i,r,o={}){this.doc=e,this.appId=i,this.nonce=r,_A(e,i,this.inline,this.external)&&this.hosts.add(e.head)}addStyles(e,i){for(let r of e)this.addUsage(r,this.inline,LS);i?.forEach(r=>this.addUsage(r,this.external,tg))}removeStyles(e,i){for(let r of e)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(e,i,r){let o=i.get(e);o?o.usage++:i.set(e,{usage:1,elements:[...this.hosts].map(s=>this.addElement(s,r(e,this.doc)))})}removeUsage(e,i){let r=i.get(e);r&&(r.usage--,r.usage<=0&&(FS(r.elements),i.delete(e)))}ngOnDestroy(){for(let[,{elements:e}]of[...this.inline,...this.external])FS(e);this.hosts.clear()}addHost(e){if(!this.hosts.has(e)){this.hosts.add(e);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(e,LS(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(e,tg(i,this.doc)))}}removeHost(e){this.hosts.delete(e);for(let i of[...this.inline.values(),...this.external.values()]){let r=[];for(let o of i.elements)o.parentNode===e?o.remove():r.push(o);i.elements=r}}addElement(e,i){return this.nonce&&i.setAttribute("nonce",this.nonce),e.appendChild(i)}static \u0275fac=function(i){return new(i||t)(A(j),A(si),A(Bi,8),A(zr))};static \u0275prov=G({token:t,factory:t.\u0275fac})}return t})(),eg={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},rg=/%COMP%/g;var BS="%COMP%",yA=`_nghost-${BS}`,SA=`_ngcontent-${BS}`,CA=!0,DA=new b("",{factory:()=>CA}),EA=new b("");function xA(t){return SA.replace(rg,t)}function wA(t){return yA.replace(rg,t)}function jS(t,n){return n.map(e=>e.replace(rg,t))}var og=(()=>{class t{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;cssVarNamespace;constructor(e,i,r,o,s,a,l=null,c=null,d=null){this.eventManager=e,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=o,this.doc=s,this.ngZone=a,this.nonce=l,this.tracingService=c,this.cssVarNamespace=d??"",this.defaultRenderer=new qa(e,s,a,this.tracingService,this.cssVarNamespace)}createRenderer(e,i){if(!e||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(e,i);return r instanceof au?r.applyToHost(e):r instanceof Ya&&r.applyStyles(),r}getOrCreateRenderer(e,i){let r=this.rendererByCompId,o=r.get(i.id);if(!o){let s=this.doc,a=this.ngZone,l=this.eventManager,c=this.sharedStylesHost,d=this.removeStylesOnCompDestroy,f=this.tracingService;switch(i.encapsulation){case yn.Emulated:o=new au(l,c,i,this.appId,d,s,a,f,this.cssVarNamespace);break;case yn.ShadowDom:return new su(l,e,i,s,a,this.nonce,f,this.cssVarNamespace,c);case yn.ExperimentalIsolatedShadowDom:return new su(l,e,i,s,a,this.nonce,f,this.cssVarNamespace);default:o=new Ya(l,c,i,d,s,a,f,this.cssVarNamespace);break}r.set(i.id,o)}return o}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(e){this.rendererByCompId.delete(e)}static \u0275fac=function(i){return new(i||t)(A(ng),A(Yr),A(si),A(DA),A(j),A(k),A(Bi),A($n,8),A(EA,8))};static \u0275prov=G({token:t,factory:t.\u0275fac})}return t})(),qa=class{eventManager;doc;ngZone;tracingService;cssVarNamespace;data=Object.create(null);throwOnSyntheticProps=!0;constructor(n,e,i,r,o=""){this.eventManager=n,this.doc=e,this.ngZone=i,this.tracingService=r,this.cssVarNamespace=o}destroy(){}destroyNode=null;createElement(n,e){return e?this.doc.createElementNS(eg[e]||e,n):this.doc.createElement(n)}createComment(n){return this.doc.createComment(n)}createText(n){return this.doc.createTextNode(n)}appendChild(n,e){(VS(n)?n.content:n).appendChild(e)}insertBefore(n,e,i){if(n){let r=VS(n)?n.content:n;if(i!=null&&i.parentNode!==r)throw new w(-5106,!1);r.insertBefore(e,i)}}removeChild(n,e){e.remove()}selectRootElement(n,e){let i=typeof n=="string"?this.doc.querySelector(n):n;if(!i)throw new w(-5104,!1);return e||(i.textContent=""),i}parentNode(n){return n.parentNode}nextSibling(n){return n.nextSibling}setAttribute(n,e,i,r){if(r){e=r+":"+e;let o=eg[r];o?n.setAttributeNS(o,e,i):n.setAttribute(e,i)}else n.setAttribute(e,i)}removeAttribute(n,e,i){if(i){let r=eg[i];r?n.removeAttributeNS(r,e):n.removeAttribute(`${i}:${e}`)}else n.removeAttribute(e)}addClass(n,e){n.classList.add(e)}removeClass(n,e){n.classList.remove(e)}setStyle(n,e,i,r){let o=e.startsWith("--");o&&(e=e.replace("%NS%",this.cssVarNamespace)),o||r&(Un.DashCase|Un.Important)?n.style.setProperty(e,i,r&Un.Important?"important":""):n.style[e]=i}removeStyle(n,e,i){let r=e.startsWith("--");r&&(e=e.replace("%NS%",this.cssVarNamespace)),r||i&Un.DashCase?n.style.removeProperty(e):n.style[e]=""}setProperty(n,e,i){n!=null&&(n[e]=i)}setValue(n,e){n.nodeValue=e}listen(n,e,i,r){if(typeof n=="string"&&(n=nn().getGlobalEventTarget(this.doc,n),!n))throw new w(-5102,!1);let o=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(o=this.tracingService.wrapEventListener(n,e,o)),this.eventManager.addEventListener(n,e,o,r)}decoratePreventDefault(n){return e=>{if(e==="__ngUnwrap__")return n;n(e)===!1&&e.preventDefault()}}};function VS(t){return t.tagName==="TEMPLATE"&&t.content!==void 0}var su=class extends qa{hostEl;sharedStylesHost;shadowRoot;constructor(n,e,i,r,o,s,a,l,c){super(n,r,o,a,l),this.hostEl=e,this.sharedStylesHost=c,this.shadowRoot=e.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let d=i.styles;d=jS(i.id,d).map(h=>h.replace(/%NS%/g,l));for(let h of d){let p=document.createElement("style");s&&p.setAttribute("nonce",s),p.textContent=h,this.shadowRoot.appendChild(p)}let f=i.getExternalStyles?.();if(f)for(let h of f){let p=tg(h,r);s&&p.setAttribute("nonce",s),this.shadowRoot.appendChild(p)}}nodeOrShadowRoot(n){return n===this.hostEl?this.shadowRoot:n}appendChild(n,e){return super.appendChild(this.nodeOrShadowRoot(n),e)}insertBefore(n,e,i){return super.insertBefore(this.nodeOrShadowRoot(n),e,i)}removeChild(n,e){return super.removeChild(null,e)}parentNode(n){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(n)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},Ya=class extends qa{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(n,e,i,r,o,s,a,l,c){super(n,o,s,a,l),this.sharedStylesHost=e,this.removeStylesOnCompDestroy=r;let d=i.styles,f=c?jS(c,d):d;this.styles=f.map(h=>h.replace(/%NS%/g,l)),this.styleUrls=i.getExternalStyles?.(c)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&Hi.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},au=class extends Ya{contentAttr;hostAttr;constructor(n,e,i,r,o,s,a,l,c){let d=r+"-"+i.id;super(n,e,i,o,s,a,l,c,d),this.contentAttr=xA(d),this.hostAttr=wA(d)}applyToHost(n){this.applyStyles(),this.setAttribute(n,this.hostAttr,"")}createElement(n,e){let i=super.createElement(n,e);return super.setAttribute(i,this.contentAttr,""),i}};var cu=class t extends $a{supportsDOMEvents=!0;static makeCurrent(){Xp(new t)}onAndCancel(n,e,i,r){return n.addEventListener(e,i,r),()=>{n.removeEventListener(e,i,r)}}dispatchEvent(n,e){n.dispatchEvent(e)}remove(n){n.remove()}createElement(n,e){return e=e||this.getDefaultDocument(),e.createElement(n)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(n){return n.nodeType===Node.ELEMENT_NODE}isShadowRoot(n){return n instanceof DocumentFragment}getGlobalEventTarget(n,e){return e==="window"?window:e==="document"?n:e==="body"?n.body:null}getBaseHref(n){let e=IA();return e==null?null:MA(e)}resetBaseElement(){Za=null}getUserAgent(){return window.navigator.userAgent}getCookie(n){return Ga(document.cookie,n)}},Za=null;function IA(){return Za=Za||document.head.querySelector("base"),Za?Za.getAttribute("href"):null}function MA(t){return new URL(t,document.baseURI).pathname}var HS=["alt","control","meta","shift"],TA={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},NA={alt:t=>t.altKey,control:t=>t.ctrlKey,meta:t=>t.metaKey,shift:t=>t.shiftKey},US=(()=>{class t extends Wa{constructor(e){super(e)}supports(e){return t.parseEventName(e)!=null}addEventListener(e,i,r,o){let s=t.parseEventName(i),a=t.eventCallback(s.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>nn().onAndCancel(e,s.domEventName,a,o))}static parseEventName(e){let i=e.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let o=t._normalizeKey(i.pop()),s="",a=i.indexOf("code");if(a>-1&&(i.splice(a,1),s="code."),HS.forEach(c=>{let d=i.indexOf(c);d>-1&&(i.splice(d,1),s+=c+".")}),s+=o,i.length!=0||o.length===0)return null;let l={};return l.domEventName=r,l.fullKey=s,l}static matchEventFullKeyCode(e,i){let r=TA[e.key]||e.key,o="";return i.indexOf("code.")>-1&&(r=e.code,o="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),HS.forEach(s=>{if(s!==r){let a=NA[s];a(e)&&(o+=s+".")}}),o+=r,o===i)}static eventCallback(e,i,r){return o=>{t.matchEventFullKeyCode(o,e)&&r.runGuarded(()=>i(o))}}static _normalizeKey(e){return e==="esc"?"escape":e}static \u0275fac=function(i){return new(i||t)(A(j))};static \u0275prov=G({token:t,factory:t.\u0275fac})}return t})();async function sg(t,n,e){let i=v({rootComponent:t},kA(n,e));return MS(i)}function kA(t,n){return{platformRef:n?.platformRef,appProviders:[...FA,...t?.providers??[]],platformProviders:PA}}function AA(){cu.makeCurrent()}function RA(){return new Ot}function OA(){return dp(document),document}var PA=[{provide:zr,useValue:Qp},{provide:md,useValue:AA,multi:!0},{provide:j,useFactory:OA}];var FA=[{provide:_a,useValue:"root"},{provide:Ot,useFactory:RA},{provide:lu,useClass:ou,multi:!0},{provide:lu,useClass:US,multi:!0},og,{provide:Yr,useClass:ig},{provide:ig,useExisting:Yr},ng,{provide:Qe,useExisting:og},[]];var hi=class t{headers;normalizedNames=new Map;lazyInit;lazyUpdate=null;constructor(n){n?typeof n=="string"?this.lazyInit=()=>{this.headers=new Map,n.split(`
`).forEach(e=>{let i=e.indexOf(":");if(i>0){let r=e.slice(0,i),o=e.slice(i+1).trim();this.addHeaderEntry(r,o)}})}:typeof Headers<"u"&&n instanceof Headers?(this.headers=new Map,n.forEach((e,i)=>{this.addHeaderEntry(i,e)})):this.lazyInit=()=>{this.headers=new Map,Object.entries(n).forEach(([e,i])=>{this.setHeaderEntries(e,i)})}:this.headers=new Map}has(n){return this.init(),this.headers.has(n.toLowerCase())}get(n){this.init();let e=this.headers.get(n.toLowerCase());return e&&e.length>0?e[0]:null}keys(){return this.init(),Array.from(this.normalizedNames.values())}getAll(n){return this.init(),this.headers.get(n.toLowerCase())||null}append(n,e){return this.clone({name:n,value:e,op:"a"})}set(n,e){return this.clone({name:n,value:e,op:"s"})}delete(n,e){return this.clone({name:n,value:e,op:"d"})}maybeSetNormalizedName(n,e){this.normalizedNames.has(e)||this.normalizedNames.set(e,n)}init(){this.lazyInit&&(this.lazyInit instanceof t?this.copyFrom(this.lazyInit):this.lazyInit(),this.lazyInit=null,this.lazyUpdate&&(this.lazyUpdate.forEach(n=>this.applyUpdate(n)),this.lazyUpdate=null))}copyFrom(n){n.init();for(let[e,i]of n.headers.entries())this.headers.set(e,i),this.normalizedNames.set(e,n.normalizedNames.get(e))}clone(n){let e=new t;return e.lazyInit=this.lazyInit&&this.lazyInit instanceof t?this.lazyInit:this,e.lazyUpdate=(this.lazyUpdate||[]).concat([n]),e}applyUpdate(n){let e=n.name.toLowerCase();switch(n.op){case"a":case"s":let i=n.value;if(typeof i=="string"&&(i=[i]),i.length===0)return;this.maybeSetNormalizedName(n.name,e);let r=n.op==="a"?(this.headers.get(e)||[]).slice():[];r.push(...i),this.headers.set(e,r);break;case"d":let o=n.value;if(o===void 0)this.headers.delete(e),this.normalizedNames.delete(e);else{let s=Array.isArray(o)?o:[o],a=this.headers.get(e);if(!a)return;a=a.filter(l=>s.indexOf(l)===-1),a.length===0?(this.headers.delete(e),this.normalizedNames.delete(e)):this.headers.set(e,a)}break}}addHeaderEntry(n,e){let i=n.toLowerCase();this.maybeSetNormalizedName(n,i),this.headers.has(i)?this.headers.get(i).push(e):this.headers.set(i,[e])}setHeaderEntries(n,e){let i=(Array.isArray(e)?e:[e]).map(o=>o.toString()),r=n.toLowerCase();this.headers.set(r,i),this.maybeSetNormalizedName(n,r)}forEach(n){this.init(),Array.from(this.normalizedNames.keys()).forEach(e=>n(this.normalizedNames.get(e),this.headers.get(e)))}};var cg=class{map=new Map;set(n,e){return this.map.set(n,e),this}get(n){return this.map.has(n)||this.map.set(n,n.defaultValue()),this.map.get(n)}delete(n){return this.map.delete(n),this}has(n){return this.map.has(n)}keys(){return this.map.keys()}},dg=class{encodeKey(n){return zS(n)}encodeValue(n){return zS(n)}decodeKey(n){return decodeURIComponent(n)}decodeValue(n){return decodeURIComponent(n)}};function LA(t,n){let e=new Map;return t.length>0&&t.replace(/^\?/,"").split("&").forEach(r=>{let o=r.indexOf("="),[s,a]=o==-1?[n.decodeKey(r),""]:[n.decodeKey(r.slice(0,o)),n.decodeValue(r.slice(o+1))],l=e.get(s)||[];l.push(a),e.set(s,l)}),e}var VA=/%(\d[a-f0-9])/gi,BA={40:"@","3A":":",24:"$","2C":",","3B":";","3D":"=","3F":"?","2F":"/"};function zS(t){return encodeURIComponent(t).replace(VA,(n,e)=>BA[e]??n)}function du(t){return`${t}`}var fi=class t{map;encoder;updates=null;cloneFrom=null;constructor(n={}){if(this.encoder=n.encoder||new dg,n.fromString){if(n.fromObject)throw new w(2805,!1);this.map=LA(n.fromString,this.encoder)}else n.fromObject?(this.map=new Map,Object.keys(n.fromObject).forEach(e=>{let i=n.fromObject[e],r=Array.isArray(i)?i.map(du):[du(i)];this.map.set(e,r)})):this.map=null}has(n){return this.init(),this.map.has(n)}get(n){this.init();let e=this.map.get(n);return e?e[0]:null}getAll(n){return this.init(),this.map.get(n)||null}keys(){return this.init(),Array.from(this.map.keys())}append(n,e){return this.clone({param:n,value:e,op:"a"})}appendAll(n){let e=[];return Object.keys(n).forEach(i=>{let r=n[i];Array.isArray(r)?r.forEach(o=>{e.push({param:i,value:o,op:"a"})}):e.push({param:i,value:r,op:"a"})}),this.clone(e)}set(n,e){return this.clone({param:n,value:e,op:"s"})}delete(n,e){return this.clone({param:n,value:e,op:"d"})}toString(){return this.init(),this.keys().map(n=>{let e=this.encoder.encodeKey(n);return this.map.get(n).map(i=>e+"="+this.encoder.encodeValue(i)).join("&")}).filter(n=>n!=="").join("&")}clone(n){let e=new t({encoder:this.encoder});return e.cloneFrom=this.cloneFrom||this,e.updates=(this.updates||[]).concat(n),e}init(){if(this.map===null&&(this.map=new Map),this.cloneFrom!==null){this.cloneFrom.init();for(let[n,e]of this.cloneFrom.map.entries())this.map.set(n,e);this.updates.forEach(n=>{switch(n.op){case"a":case"s":let e=n.op==="a"?(this.map.get(n.param)||[]).slice():[];e.push(du(n.value)),this.map.set(n.param,e);break;case"d":if(n.value!==void 0){let i=(this.map.get(n.param)||[]).slice(),r=i.indexOf(du(n.value));r!==-1&&i.splice(r,1),i.length>0?this.map.set(n.param,i):this.map.delete(n.param)}else{this.map.delete(n.param);break}}}),this.cloneFrom=this.updates=null}}};function jA(t){switch(t){case"DELETE":case"GET":case"HEAD":case"OPTIONS":case"JSONP":return!1;default:return!0}}function $S(t){return typeof ArrayBuffer<"u"&&t instanceof ArrayBuffer}function GS(t){return typeof Blob<"u"&&t instanceof Blob}function WS(t){return typeof FormData<"u"&&t instanceof FormData}function HA(t){return typeof URLSearchParams<"u"&&t instanceof URLSearchParams}var ag="Content-Type",qS="Accept",XS="text/plain",KS="application/json",UA=`${KS}, ${XS}, */*`,is=class t{url;body=null;headers;context;reportProgress=!1;reportUploadProgress=!1;reportDownloadProgress=!1;withCredentials=!1;credentials;keepalive=!1;cache;priority;mode;redirect;referrer;integrity;referrerPolicy;responseType="json";method;params;urlWithParams;transferCache;timeout;constructor(n,e,i,r){this.url=e,this.method=n.toUpperCase();let o;if(jA(this.method)||r?(this.body=i!==void 0?i:null,o=r):o=i,o){if(this.reportProgress=!!o.reportProgress,this.reportUploadProgress=!!o.reportUploadProgress,this.reportDownloadProgress=!!o.reportDownloadProgress,this.withCredentials=!!o.withCredentials,this.keepalive=!!o.keepalive,o.responseType&&(this.responseType=o.responseType),o.headers&&(this.headers=o.headers),o.context&&(this.context=o.context),o.params&&(this.params=o.params),o.priority&&(this.priority=o.priority),o.cache&&(this.cache=o.cache),o.credentials&&(this.credentials=o.credentials),typeof o.timeout=="number"){if(o.timeout<1||!Number.isInteger(o.timeout))throw new w(2822,"");this.timeout=o.timeout}o.mode&&(this.mode=o.mode),o.redirect&&(this.redirect=o.redirect),o.integrity&&(this.integrity=o.integrity),o.referrer!==void 0&&(this.referrer=o.referrer),o.referrerPolicy&&(this.referrerPolicy=o.referrerPolicy),this.transferCache=o.transferCache}if(this.headers??=new hi,this.context??=new cg,!this.params)this.params=new fi,this.urlWithParams=e;else{let s=this.params.toString();if(s.length===0)this.urlWithParams=e;else{let a=e,l="",c=e.indexOf("#");c!==-1&&(l=e.substring(c),a=e.substring(0,c));let d=a.indexOf("?"),f=d===-1?"?":d<a.length-1?"&":"";this.urlWithParams=a+f+s+l}}}serializeBody(){return this.body===null?null:typeof this.body=="string"||$S(this.body)||GS(this.body)||WS(this.body)||HA(this.body)?this.body:this.body instanceof fi?this.body.toString():typeof this.body=="object"||typeof this.body=="boolean"||Array.isArray(this.body)?JSON.stringify(this.body):this.body.toString()}detectContentTypeHeader(){return this.body===null||WS(this.body)?null:GS(this.body)?this.body.type||null:$S(this.body)?null:typeof this.body=="string"?XS:this.body instanceof fi?"application/x-www-form-urlencoded;charset=UTF-8":typeof this.body=="object"||typeof this.body=="number"||typeof this.body=="boolean"?KS:null}clone(n={}){let e=n.method||this.method,i=n.url||this.url,r=n.responseType||this.responseType,o=n.keepalive??this.keepalive,s=n.priority||this.priority,a=n.cache||this.cache,l=n.mode||this.mode,c=n.redirect||this.redirect,d=n.credentials||this.credentials,f=n.referrer??this.referrer,h=n.integrity||this.integrity,p=n.referrerPolicy||this.referrerPolicy,S=n.transferCache??this.transferCache,E=n.timeout??this.timeout,$=n.body!==void 0?n.body:this.body,H=n.withCredentials??this.withCredentials,Se=n.reportProgress??this.reportProgress,Ge=n.reportUploadProgress??this.reportUploadProgress,Ws=n.reportDownloadProgress??this.reportDownloadProgress,Ii=n.headers||this.headers,qs=n.params||this.params,Ys=n.context??this.context;return n.setHeaders!==void 0&&(Ii=Object.keys(n.setHeaders).reduce((xo,Mi)=>xo.set(Mi,n.setHeaders[Mi]),Ii)),n.setParams&&(qs=Object.keys(n.setParams).reduce((xo,Mi)=>xo.set(Mi,n.setParams[Mi]),qs)),new t(e,i,$,{params:qs,headers:Ii,context:Ys,reportProgress:Se,reportUploadProgress:Ge,reportDownloadProgress:Ws,responseType:r,withCredentials:H,transferCache:S,keepalive:o,cache:a,priority:s,timeout:E,mode:l,redirect:c,credentials:d,referrer:f,integrity:h,referrerPolicy:p})}},Jr=(function(t){return t[t.Sent=0]="Sent",t[t.UploadProgress=1]="UploadProgress",t[t.ResponseHeader=2]="ResponseHeader",t[t.DownloadProgress=3]="DownloadProgress",t[t.Response=4]="Response",t[t.User=5]="User",t})(Jr||{}),Xa=class{headers;status;statusText;url;ok;type;redirected;responseType;constructor(n,e=200,i="OK"){this.headers=n.headers||new hi,this.status=n.status!==void 0?n.status:e,this.statusText=n.statusText||i,this.url=n.url||null,this.redirected=n.redirected,this.responseType=n.responseType,this.ok=this.status>=200&&this.status<300}},ug=class t extends Xa{constructor(n={}){super(n)}type=Jr.ResponseHeader;clone(n={}){return new t({headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0})}},Ka=class t extends Xa{body;constructor(n={}){super(n),this.body=n.body!==void 0?n.body:null}type=Jr.Response;clone(n={}){return new t({body:n.body!==void 0?n.body:this.body,headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0,redirected:n.redirected??this.redirected,responseType:n.responseType??this.responseType})}},Qr=class extends Xa{name="HttpErrorResponse";message;error;ok=!1;constructor(n){super(n,0,"Unknown Error"),this.status>=200&&this.status<300?this.message=`Http failure during parsing for ${n.url||"(unknown url)"}`:this.message=`Http failure response for ${n.url||"(unknown url)"}: ${n.status} ${n.statusText}`,this.error=n.error||null}},zA=200;var $A=/^\)\]\}',?\n/,bY=1024*1024,GA=new b("",{factory:()=>null}),WA=(()=>{class t{fetchImpl=u(fg,{optional:!0})?.fetch??((...e)=>globalThis.fetch(...e));ngZone=u(k);destroyRef=u(qe);maxResponseSize=u(GA);handle(e){return new ne(i=>{let r=new AbortController,o=!1,s={next:l=>{l.type===Jr.Response&&(o=!0),i.next(l)},error:l=>{o=!0,i.error(l)},complete:()=>{o=!0,i.complete()}};this.doRequest(e,r.signal,s).then(hg,l=>s.error(new Qr({error:l})));let a;return e.timeout&&(a=this.ngZone.runOutsideAngular(()=>setTimeout(()=>{r.signal.aborted||r.abort(new DOMException("signal timed out","TimeoutError"))},e.timeout))),()=>{a!==void 0&&clearTimeout(a),!o&&!r.signal.aborted&&r.abort()}})}async doRequest(e,i,r){let o=this.createRequestInit(e),s;try{let $=this.ngZone.runOutsideAngular(()=>this.fetchImpl(e.urlWithParams,v({signal:i},o)));qA($),r.next({type:Jr.Sent}),s=await $}catch($){r.error(new Qr({error:$,status:$.status??0,statusText:$.statusText,url:e.urlWithParams,headers:$.headers}));return}let a=new hi(s.headers),l=s.statusText,c=s.url||e.urlWithParams,d=s.status,f=null,h=e.reportProgress||e.reportDownloadProgress;if(h&&r.next(new ug({headers:a,status:d,statusText:l,url:c})),s.body){let $=s.headers.get(ag)??"",H=s.headers.get("content-length"),Se=H!==null?Number(H):NaN;this.maxResponseSize!==null&&Number.isFinite(Se)&&Se>this.maxResponseSize&&(await s.body.cancel(),YS(this.maxResponseSize));let Ge=[],Ws=s.body.getReader(),Ii=0,qs,Ys,xo=typeof Zone<"u"&&Zone.current,Mi=!1;if(await this.ngZone.runOutsideAngular(async()=>{for(;;){if(this.destroyRef.destroyed){await Ws.cancel(),Mi=!0;break}let{done:Wf,value:qf}=await Ws.read();if(Wf)break;if(Ge.push(qf),Ii+=qf.length,this.maxResponseSize!==null&&Ii>this.maxResponseSize&&(await Ws.cancel(),YS(this.maxResponseSize)),h){Ys=e.responseType==="text"?(Ys??"")+(qs??=ZS($)).decode(qf,{stream:!0}):void 0;let Zv=()=>r.next({type:Jr.DownloadProgress,total:Number.isFinite(Se)?Se:void 0,loaded:Ii,partialText:Ys});xo?xo.run(Zv):Zv()}}}),Mi){r.complete();return}let ew=this.concatChunks(Ge,Ii);try{f=this.parseBody(e,ew,$,d)}catch(Wf){r.error(new Qr({error:Wf,headers:new hi(s.headers),status:s.status,statusText:s.statusText,url:s.url||e.urlWithParams}));return}}d===0&&(d=f?zA:0);let p=d>=200&&d<300,S=s.redirected,E=s.type;p?(r.next(new Ka({body:f,headers:a,status:d,statusText:l,url:c,redirected:S,responseType:E})),r.complete()):r.error(new Qr({error:f,headers:a,status:d,statusText:l,url:c,redirected:S,responseType:E}))}parseBody(e,i,r,o){switch(e.responseType){case"json":let s=new TextDecoder().decode(i).replace($A,"");if(s==="")return null;try{return JSON.parse(s)}catch(a){if(o<200||o>=300)return s;throw a}case"text":return ZS(r).decode(i);case"blob":return new Blob([i],{type:r});case"arraybuffer":return i.buffer}}createRequestInit(e){if(e.reportUploadProgress)throw new w(2824,!1);let i={},r;if(r=e.credentials,e.withCredentials&&(r="include"),e.headers.forEach((o,s)=>i[o]=s.join(",")),e.headers.has(qS)||(i[qS]=UA),!e.headers.has(ag)){let o=e.detectContentTypeHeader();o!==null&&(i[ag]=o)}return{body:e.serializeBody(),method:e.method,headers:i,credentials:r,keepalive:e.keepalive,cache:e.cache,priority:e.priority,mode:e.mode,redirect:e.redirect,referrer:e.referrer,integrity:e.integrity,referrerPolicy:e.referrerPolicy}}concatChunks(e,i){let r=new Uint8Array(i),o=0;for(let s of e)r.set(s,o),o+=s.length;return r}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})(),fg=class{};function hg(){}function qA(t){t.then(hg,hg)}function YS(t){throw new w(-2825,!1)}var YA=/charset=\s*["']?([^;"'\s]+)["']?/i;function ZS(t){let n=t.match(YA);if(n!==null)try{return new TextDecoder(n[1])}catch{}return new TextDecoder}var ZA=new b("",{factory:()=>!0}),XA="XSRF-TOKEN",KA=new b("",{factory:()=>XA}),QA="X-XSRF-TOKEN",JA=new b("",{factory:()=>QA}),eR=(()=>{class t{cookieName=u(KA);doc=u(j);lastCookieString="";lastToken=null;parseCount=0;getToken(){let e=this.doc.cookie||"";return e!==this.lastCookieString&&(this.parseCount++,this.lastToken=Ga(e,this.cookieName),this.lastCookieString=e),this.lastToken}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})(),tR=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=G({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=A(eR),r},providedIn:"root"})}return t})();function nR(t,n){if(!u(ZA)||t.method==="GET"||t.method==="HEAD")return n(t);try{let r=u(ns).href,{origin:o}=new URL(r),{origin:s}=new URL(t.url,o);if(o!==s)return n(t)}catch{return n(t)}let e=u(tR).getToken(),i=u(JA);return e!=null&&!t.headers.has(i)&&(t=t.clone({headers:t.headers.set(i,e)})),n(t)}function iR(t,n){return n(t)}function rR(t,n,e){return(i,r)=>ot(e,()=>n(i,o=>t(o,r)))}var oR=new b("",{factory:()=>[nR]}),QS=new b(""),sR=new b("",{factory:()=>!0});var aR=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=G({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=A(WA),r},providedIn:"root"})}return t})();var lR=(()=>{class t{backend;injector;chain=null;pendingTasks=u(gd);contributeToStability=u(sR);constructor(e,i){this.backend=e,this.injector=i}handle(e){if(this.chain===null){let r=this.injector.get(JS,null,{skipSelf:!0}),o=r!==null&&this.backend===r,s=this.injector.get(QS,[],o?{self:!0}:void 0),a=Array.from(new Set([...this.injector.get(oR),...s]));this.chain=a.reduceRight((l,c)=>rR(l,c,this.injector),iR)}let i=this.chain;if(this.contributeToStability){let r=this.pendingTasks.add();return Ne(()=>i(e,o=>this.backend.handle(o))).pipe(Ir(r))}else return Ne(()=>i(e,r=>this.backend.handle(r)))}static \u0275fac=function(i){return new(i||t)(A(aR),A(Me))};static \u0275prov=G({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),JS=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=G({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=A(lR),r},providedIn:"root"})}return t})();function lg(t,n){return v({body:n},t)}var mg=(()=>{class t{handler;constructor(e){this.handler=e}request(e,i,r={}){let o;if(e instanceof is)o=e;else{let l;r.headers instanceof hi?l=r.headers:l=new hi(r.headers);let c;r.params&&(r.params instanceof fi?c=r.params:c=new fi({fromObject:r.params})),o=new is(e,i,r.body!==void 0?r.body:null,{headers:l,context:r.context,params:c,reportProgress:r.reportProgress,reportUploadProgress:r.reportUploadProgress,reportDownloadProgress:r.reportDownloadProgress,responseType:r.responseType||"json",withCredentials:r.withCredentials,transferCache:r.transferCache,keepalive:r.keepalive,priority:r.priority,cache:r.cache,mode:r.mode,redirect:r.redirect,credentials:r.credentials,referrer:r.referrer,referrerPolicy:r.referrerPolicy,integrity:r.integrity,timeout:r.timeout})}let s=Y(o).pipe(Oo(l=>this.handler.handle(l)));if(e instanceof is||r.observe==="events")return s;let a=s.pipe(ge(l=>l instanceof Ka));switch(r.observe||"body"){case"body":switch(o.responseType){case"arraybuffer":return a.pipe(oe(l=>{if(l.body!==null&&!(l.body instanceof ArrayBuffer))throw new w(2806,!1);return l.body}));case"blob":return a.pipe(oe(l=>{if(l.body!==null&&!(l.body instanceof Blob))throw new w(2807,!1);return l.body}));case"text":return a.pipe(oe(l=>{if(l.body!==null&&typeof l.body!="string")throw new w(2808,!1);return l.body}));default:return a.pipe(oe(l=>l.body))}case"response":return a;default:throw new w(2809,!1)}}delete(e,i={}){return this.request("DELETE",e,i)}get(e,i={}){return this.request("GET",e,i)}head(e,i={}){return this.request("HEAD",e,i)}jsonp(e,i){return this.request("JSONP",e,{params:new fi().append(i,"JSONP_CALLBACK"),observe:"body",responseType:"json"})}options(e,i={}){return this.request("OPTIONS",e,i)}patch(e,i,r={}){return this.request("PATCH",e,lg(r,i))}post(e,i,r={}){return this.request("POST",e,lg(r,i))}put(e,i,r={}){return this.request("PUT",e,lg(r,i))}static \u0275fac=function(i){return new(i||t)(A(JS))};static \u0275prov=G({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var eC=(()=>{class t{_doc;constructor(e){this._doc=e}getTitle(){return this._doc.title}setTitle(e){this._doc.title=e||""}static \u0275fac=function(i){return new(i||t)(A(j))};static \u0275prov=G({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Qa=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=G({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=A(dR),r},providedIn:"root"})}return t})(),dR=(()=>{class t extends Qa{_doc=u(j);sanitize(e,i){if(i==null)return null;switch(e){case st.NONE:return i;case st.HTML:return zi(i,"HTML")?Sn(i):gp(this._doc,String(i)).toString();case st.STYLE:return zi(i,"Style")?Sn(i):i;case st.SCRIPT:if(zi(i,"Script"))return Sn(i);throw new w(5200,!1);case st.URL:return zi(i,"URL")?Sn(i):Fa(String(i));case st.RESOURCE_URL:if(zi(i,"ResourceURL"))return Sn(i);throw new w(-5201,!1);default:throw new w(5202,!1)}}bypassSecurityTrustHtml(e){return up(e)}bypassSecurityTrustStyle(e){return fp(e)}bypassSecurityTrustScript(e){return hp(e)}bypassSecurityTrustUrl(e){return mp(e)}bypassSecurityTrustResourceUrl(e){return pp(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();var ae="primary",fl=Symbol("RouteTitle"),_g=class{params;constructor(n){this.params=n||{}}has(n){return Object.hasOwn(this.params,n)}get(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e[0]:e}return null}getAll(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e:[e]}return[]}get keys(){return Object.keys(this.params)}};function to(t){return new _g(t)}function pg(t,n,e){for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(r[0]===":")e[r.substring(1)]=o;else if(r!==o.path)return!1}return!0}function cC(t,n,e){let i=e.path.split("/"),r=i.indexOf("**");if(r===-1){if(i.length>t.length||e.pathMatch==="full"&&(n.hasChildren()||i.length<t.length))return null;let l={},c=t.slice(0,i.length);return pg(i,c,l)?{consumed:c,posParams:l}:null}if(r!==i.lastIndexOf("**"))return null;let o=i.slice(0,r),s=i.slice(r+1);if(o.length+s.length>t.length||e.pathMatch==="full"&&n.hasChildren()&&e.path!=="**")return null;let a={};return!pg(o,t.slice(0,o.length),a)||!pg(s,t.slice(t.length-s.length),a)?null:{consumed:t,posParams:a}}function gu(t){return new Promise((n,e)=>{t.pipe(Kn()).subscribe({next:i=>n(i),error:i=>e(i)})})}function fR(t,n){if(t.length!==n.length)return!1;for(let e=0;e<t.length;++e)if(!Wn(t[e],n[e]))return!1;return!0}function Wn(t,n){let e=t?yg(t):void 0,i=n?yg(n):void 0;if(!e||!i||e.length!=i.length)return!1;let r;for(let o=0;o<e.length;o++)if(r=e[o],!dC(t[r],n[r]))return!1;return!0}function yg(t){return[...Object.keys(t),...Object.getOwnPropertySymbols(t)]}function dC(t,n){if(Array.isArray(t)&&Array.isArray(n)){if(t.length!==n.length)return!1;let e=[...t].sort(),i=[...n].sort();return e.every((r,o)=>i[o]===r)}else return t===n}function hR(t){return t.length>0?t[t.length-1]:null}function oo(t){return na(t)?t:di(t)?Ve(Promise.resolve(t)):Y(t)}function uC(t){return na(t)?gu(t):Promise.resolve(t)}var mR={exact:mC,subset:pC},fC={exact:pR,subset:gR,ignored:()=>!0},hC={paths:"exact",fragment:"ignored",matrixParams:"ignored",queryParams:"exact"},Sg={paths:"subset",fragment:"ignored",matrixParams:"ignored",queryParams:"subset"};function nC(t,n,e){return mR[e.paths](t.root,n.root,e.matrixParams)&&fC[e.queryParams](t.queryParams,n.queryParams)&&!(e.fragment==="exact"&&t.fragment!==n.fragment)}function pR(t,n){return Wn(t,n)}function mC(t,n,e){if(!eo(t.segments,n.segments)||!hu(t.segments,n.segments,e)||t.numberOfChildren!==n.numberOfChildren)return!1;for(let i in n.children)if(!t.children[i]||!mC(t.children[i],n.children[i],e))return!1;return!0}function gR(t,n){return Object.keys(n).length<=Object.keys(t).length&&Object.keys(n).every(e=>dC(t[e],n[e]))}function pC(t,n,e){return gC(t,n,n.segments,e)}function gC(t,n,e,i){if(t.segments.length>e.length){let r=t.segments.slice(0,e.length);return!(!eo(r,e)||n.hasChildren()||!hu(r,e,i))}else if(t.segments.length===e.length){if(!eo(t.segments,e)||!hu(t.segments,e,i))return!1;for(let r in n.children)if(!t.children[r]||!pC(t.children[r],n.children[r],i))return!1;return!0}else{let r=e.slice(0,t.segments.length),o=e.slice(t.segments.length);return!eo(t.segments,r)||!hu(t.segments,r,i)||!t.children[ae]?!1:gC(t.children[ae],n,o,i)}}function hu(t,n,e){return n.every((i,r)=>fC[e](t[r].parameters,i.parameters))}var on=class{root;queryParams;fragment;_queryParamMap;constructor(n=new De([],{}),e={},i=null){this.root=n,this.queryParams=e,this.fragment=i}get queryParamMap(){return this._queryParamMap??=to(this.queryParams),this._queryParamMap}toString(){return _R.serialize(this)}},De=class{segments;children;parent=null;constructor(n,e){this.segments=n,this.children=e,Object.values(e).forEach(i=>i.parent=this)}hasChildren(){return this.numberOfChildren>0}get numberOfChildren(){return Object.keys(this.children).length}toString(){return mu(this)}},Xi=class{path;parameters;_parameterMap;constructor(n,e){this.path=n,this.parameters=e}get parameterMap(){return this._parameterMap??=to(this.parameters),this._parameterMap}toString(){return bC(this)}};function vR(t,n){return eo(t,n)&&t.every((e,i)=>Wn(e.parameters,n[i].parameters))}function eo(t,n){return t.length!==n.length?!1:t.every((e,i)=>e.path===n[i].path)}function bR(t,n){let e=[];return Object.entries(t.children).forEach(([i,r])=>{i===ae&&(e=e.concat(n(r,i)))}),Object.entries(t.children).forEach(([i,r])=>{i!==ae&&(e=e.concat(n(r,i)))}),e}var hl=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:()=>new Ki})}return t})(),Ki=class{parse(n){let e=new Dg(n);return new on(e.parseRootSegment(),e.parseQueryParams(),e.parseFragment())}serialize(n){let e=`/${Ja(n.root,!0)}`,i=CR(n.queryParams),r=typeof n.fragment=="string"?`#${yR(n.fragment)}`:"";return`${e}${i}${r}`}},_R=new Ki;function mu(t){return t.segments.map(n=>bC(n)).join("/")}function Ja(t,n){if(!t.hasChildren())return mu(t);if(n){let e=t.children[ae]?Ja(t.children[ae],!1):"",i=[];return Object.entries(t.children).forEach(([r,o])=>{r!==ae&&i.push(`${r}:${Ja(o,!1)}`)}),i.length>0?`${e}(${i.join("//")})`:e}else{let e=bR(t,(i,r)=>r===ae?[Ja(t.children[ae],!1)]:[`${r}:${Ja(i,!1)}`]);return Object.keys(t.children).length===1&&t.children[ae]!=null?`${mu(t)}/${e[0]}`:`${mu(t)}/(${e.join("//")})`}}function vC(t){return encodeURIComponent(t).replace(/%40/g,"@").replace(/%3A/gi,":").replace(/%24/g,"$").replace(/%2C/gi,",")}function uu(t){return vC(t).replace(/%3B/gi,";")}function yR(t){return encodeURI(t)}function Cg(t){return vC(t).replace(/\(/g,"%28").replace(/\)/g,"%29").replace(/%26/gi,"&")}function pu(t){return decodeURIComponent(t)}function iC(t){return pu(t.replace(/\+/g,"%20"))}function bC(t){return`${Cg(t.path)}${SR(t.parameters)}`}function SR(t){return Object.entries(t).map(([n,e])=>`;${Cg(n)}=${Cg(e)}`).join("")}function CR(t){let n=Object.entries(t).map(([e,i])=>Array.isArray(i)?i.map(r=>`${uu(e)}=${uu(r)}`).join("&"):`${uu(e)}=${uu(i)}`).filter(e=>e);return n.length?`?${n.join("&")}`:""}var DR=/^[^\/()?;#]+/;function gg(t){let n=t.match(DR);return n?n[0]:""}var ER=/^[^\/()?;=#]+/;function xR(t){let n=t.match(ER);return n?n[0]:""}var wR=/^[^=?&#]+/;function IR(t){let n=t.match(wR);return n?n[0]:""}var MR=/^[^&#]+/;function TR(t){let n=t.match(MR);return n?n[0]:""}var Dg=class{url;remaining;constructor(n){this.url=n,this.remaining=n}parseRootSegment(){for(;this.consumeOptional("/"););return this.remaining===""||this.peekStartsWith("?")||this.peekStartsWith("#")?new De([],{}):new De([],this.parseChildren())}parseQueryParams(){let n={};if(this.consumeOptional("?"))do this.parseQueryParam(n);while(this.consumeOptional("&"));return n}parseFragment(){return this.consumeOptional("#")?decodeURIComponent(this.remaining):null}parseChildren(n=0){if(n>50)throw new w(4010,!1);if(this.remaining==="")return{};this.consumeOptional("/");let e=[];for(this.peekStartsWith("(")||e.push(this.parseSegment());this.peekStartsWith("/")&&!this.peekStartsWith("//")&&!this.peekStartsWith("/(");)this.capture("/"),e.push(this.parseSegment());let i={};this.peekStartsWith("/(")&&(this.capture("/"),i=this.parseParens(!0,n));let r={};return this.peekStartsWith("(")&&(r=this.parseParens(!1,n)),(e.length>0||Object.keys(i).length>0)&&(r[ae]=new De(e,i)),r}parseSegment(){let n=gg(this.remaining);if(n===""&&this.peekStartsWith(";"))throw new w(4009,!1);return this.capture(n),new Xi(pu(n),this.parseMatrixParams())}parseMatrixParams(){let n={};for(;this.consumeOptional(";");)this.parseParam(n);return n}parseParam(n){let e=xR(this.remaining);if(!e)return;this.capture(e);let i="";if(this.consumeOptional("=")){let r=gg(this.remaining);r&&(i=r,this.capture(i))}n[pu(e)]=pu(i)}parseQueryParam(n){let e=IR(this.remaining);if(!e)return;this.capture(e);let i="";if(this.consumeOptional("=")){let s=TR(this.remaining);s&&(i=s,this.capture(i))}let r=iC(e),o=iC(i);if(Object.hasOwn(n,r)){let s=n[r];Array.isArray(s)||(s=[s],n[r]=s),s.push(o)}else n[r]=o}parseParens(n,e){let i=Object.create(null);for(this.capture("(");!this.consumeOptional(")")&&this.remaining.length>0;){let r=gg(this.remaining),o=this.remaining[r.length];if(o!=="/"&&o!==")"&&o!==";")throw new w(4010,!1);let s;r.indexOf(":")>-1?(s=r.slice(0,r.indexOf(":")),this.capture(s),this.capture(":")):n&&(s=ae);let a=this.parseChildren(e+1);i[s??ae]=Object.keys(a).length===1&&a[ae]?a[ae]:new De([],a),this.consumeOptional("//")}return i}peekStartsWith(n){return this.remaining.startsWith(n)}consumeOptional(n){return this.peekStartsWith(n)?(this.remaining=this.remaining.substring(n.length),!0):!1}capture(n){if(!this.consumeOptional(n))throw new w(4011,!1)}};function _C(t){return t.segments.length>0?new De([],{[ae]:t}):t}function yC(t){let n=Object.create(null);for(let[i,r]of Object.entries(t.children)){let o=yC(r);if(i===ae&&o.segments.length===0&&o.hasChildren())for(let[s,a]of Object.entries(o.children))n[s]=a;else(o.segments.length>0||o.hasChildren())&&(n[i]=o)}let e=new De(t.segments,n);return NR(e)}function NR(t){if(t.numberOfChildren===1&&t.children[ae]){let n=t.children[ae];return new De(t.segments.concat(n.segments),n.children)}return t}function as(t){return t instanceof on}function SC(t,n,e=null,i=null,r=new Ki){let o=CC(t);return DC(o,n,e,i,r)}function CC(t){let n;function e(o){let s={};for(let l of o.children){let c=e(l);s[l.outlet]=c}let a=new De(o.url,s);return o===t&&(n=a),a}let i=e(t.root),r=_C(i);return n??r}function DC(t,n,e,i,r){let o=t;for(;o.parent;)o=o.parent;if(n.length===0)return vg(o,o,o,e,i,r);let s=kR(n);if(s.toRoot())return vg(o,o,new De([],{}),e,i,r);let a=AR(s,o,t),l=a.processChildren?tl(a.segmentGroup,a.index,s.commands):xC(a.segmentGroup,a.index,s.commands);return vg(o,a.segmentGroup,l,e,i,r)}function vu(t){return typeof t=="object"&&t!=null&&!t.outlets&&!t.segmentPath}function rl(t){return typeof t=="object"&&t!=null&&t.outlets}function rC(t,n,e){t||="\u0275";let i=new on;return i.queryParams={[t]:n},e.parse(e.serialize(i)).queryParams[t]}function vg(t,n,e,i,r,o){let s={};for(let[c,d]of Object.entries(i??{}))s[c]=Array.isArray(d)?d.map(f=>rC(c,f,o)):rC(c,d,o);let a;t===n?a=e:a=EC(t,n,e);let l=_C(yC(a));return new on(l,s,r)}function EC(t,n,e){let i=Object.create(null);return Object.entries(t.children).forEach(([r,o])=>{o===n?i[r]=e:i[r]=EC(o,n,e)}),new De(t.segments,i)}var bu=class{isAbsolute;numberOfDoubleDots;commands;constructor(n,e,i){if(this.isAbsolute=n,this.numberOfDoubleDots=e,this.commands=i,n&&i.length>0&&vu(i[0]))throw new w(4003,!1);let r=i.find(rl);if(r&&r!==hR(i))throw new w(4004,!1)}toRoot(){return this.isAbsolute&&this.commands.length===1&&this.commands[0]=="/"}};function kR(t){if(typeof t[0]=="string"&&t.length===1&&t[0]==="/")return new bu(!0,0,t);let n=0,e=!1,i=t.reduce((r,o,s)=>{if(typeof o=="object"&&o!=null){if(o.outlets){let a={};return Object.entries(o.outlets).forEach(([l,c])=>{a[l]=typeof c=="string"?c.split("/"):c}),[...r,{outlets:a}]}if(o.segmentPath)return[...r,o.segmentPath]}return typeof o!="string"?[...r,o]:s===0?(o.split("/").forEach((a,l)=>{l==0&&a==="."||(l==0&&a===""?e=!0:a===".."?n++:a!=""&&r.push(a))}),r):[...r,o]},[]);return new bu(e,n,i)}var os=class{segmentGroup;processChildren;index;constructor(n,e,i){this.segmentGroup=n,this.processChildren=e,this.index=i}};function AR(t,n,e){if(t.isAbsolute)return new os(n,!0,0);if(!e)return new os(n,!1,NaN);if(e.parent===null)return new os(e,!0,0);let i=vu(t.commands[0])?0:1,r=e.segments.length-1+i;return RR(e,r,t.numberOfDoubleDots)}function RR(t,n,e){let i=t,r=n,o=e;for(;o>r;){if(o-=r,i=i.parent,!i)throw new w(4005,!1);r=i.segments.length}return new os(i,!1,r-o)}function OR(t){return rl(t[0])?t[0].outlets:{[ae]:t}}function xC(t,n,e){if(t??=new De([],{}),t.segments.length===0&&t.hasChildren())return tl(t,n,e);let i=PR(t,n,e),r=e.slice(i.commandIndex);if(i.match&&i.pathIndex<t.segments.length){let o=new De(t.segments.slice(0,i.pathIndex),{});return o.children[ae]=new De(t.segments.slice(i.pathIndex),t.children),tl(o,0,r)}else return i.match&&r.length===0?new De(t.segments,{}):i.match&&!t.hasChildren()?Eg(t,n,e):i.match?tl(t,0,r):Eg(t,n,e)}function tl(t,n,e){if(e.length===0)return new De(t.segments,{});{let i=OR(e),r=Object.create(null);if(Object.keys(i).some(o=>o!==ae)&&t.children[ae]&&t.numberOfChildren===1&&t.children[ae].segments.length===0){let o=tl(t.children[ae],n,e);return new De(t.segments,o.children)}return Object.entries(i).forEach(([o,s])=>{typeof s=="string"&&(s=[s]),s!==null&&(r[o]=xC(t.children[o],n,s))}),Object.entries(t.children).forEach(([o,s])=>{i[o]===void 0&&(r[o]=s)}),new De(t.segments,r)}}function PR(t,n,e){let i=0,r=n,o={match:!1,pathIndex:0,commandIndex:0};for(;r<t.segments.length;){if(i>=e.length)return o;let s=t.segments[r],a=e[i];if(rl(a))break;let l=`${a}`,c=i<e.length-1?e[i+1]:null;if(r>0&&l===void 0)break;if(l&&c&&typeof c=="object"&&c.outlets===void 0){if(!sC(l,c,s))return o;i+=2}else{if(!sC(l,{},s))return o;i++}r++}return{match:!0,pathIndex:r,commandIndex:i}}function Eg(t,n,e){let i=t.segments.slice(0,n),r=0;for(;r<e.length;){let o=e[r];if(rl(o)){let l=FR(o.outlets);return new De(i,l)}if(r===0&&vu(e[0])){let l=t.segments[n];i.push(new Xi(l.path,oC(e[0]))),r++;continue}let s=rl(o)?o.outlets[ae]:`${o}`,a=r<e.length-1?e[r+1]:null;s&&a&&vu(a)?(i.push(new Xi(s,oC(a))),r+=2):(i.push(new Xi(s,{})),r++)}return new De(i,{})}function FR(t){let n={};return Object.entries(t).forEach(([e,i])=>{typeof i=="string"&&(i=[i]),i!==null&&(n[e]=Eg(new De([],{}),0,i))}),n}function oC(t){let n={};return Object.entries(t).forEach(([e,i])=>n[e]=`${i}`),n}function sC(t,n,e){return t==e.path&&Wn(n,e.parameters)}var nl="imperative",mt=(function(t){return t[t.NavigationStart=0]="NavigationStart",t[t.NavigationEnd=1]="NavigationEnd",t[t.NavigationCancel=2]="NavigationCancel",t[t.NavigationError=3]="NavigationError",t[t.RoutesRecognized=4]="RoutesRecognized",t[t.ResolveStart=5]="ResolveStart",t[t.ResolveEnd=6]="ResolveEnd",t[t.GuardsCheckStart=7]="GuardsCheckStart",t[t.GuardsCheckEnd=8]="GuardsCheckEnd",t[t.RouteConfigLoadStart=9]="RouteConfigLoadStart",t[t.RouteConfigLoadEnd=10]="RouteConfigLoadEnd",t[t.ChildActivationStart=11]="ChildActivationStart",t[t.ChildActivationEnd=12]="ChildActivationEnd",t[t.ActivationStart=13]="ActivationStart",t[t.ActivationEnd=14]="ActivationEnd",t[t.Scroll=15]="Scroll",t[t.NavigationSkipped=16]="NavigationSkipped",t})(mt||{}),Gt=class{id;url;constructor(n,e){this.id=n,this.url=e}},no=class extends Gt{type=mt.NavigationStart;navigationTrigger;restoredState;constructor(n,e,i="imperative",r=null){super(n,e),this.navigationTrigger=i,this.restoredState=r}toString(){return`NavigationStart(id: ${this.id}, url: '${this.url}')`}},pi=class extends Gt{urlAfterRedirects;type=mt.NavigationEnd;constructor(n,e,i){super(n,e),this.urlAfterRedirects=i}toString(){return`NavigationEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}')`}},wt=(function(t){return t[t.Redirect=0]="Redirect",t[t.SupersededByNewNavigation=1]="SupersededByNewNavigation",t[t.NoDataFromResolver=2]="NoDataFromResolver",t[t.GuardRejected=3]="GuardRejected",t[t.Aborted=4]="Aborted",t})(wt||{}),ol=(function(t){return t[t.IgnoredSameUrlNavigation=0]="IgnoredSameUrlNavigation",t[t.IgnoredByUrlHandlingStrategy=1]="IgnoredByUrlHandlingStrategy",t})(ol||{}),rn=class extends Gt{reason;code;type=mt.NavigationCancel;constructor(n,e,i,r){super(n,e),this.reason=i,this.code=r}toString(){return`NavigationCancel(id: ${this.id}, url: '${this.url}')`}};function wC(t){return t instanceof rn&&(t.code===wt.Redirect||t.code===wt.SupersededByNewNavigation)}var gi=class extends Gt{reason;code;type=mt.NavigationSkipped;constructor(n,e,i,r){super(n,e),this.reason=i,this.code=r}},io=class extends Gt{error;target;type=mt.NavigationError;constructor(n,e,i,r){super(n,e),this.error=i,this.target=r}toString(){return`NavigationError(id: ${this.id}, url: '${this.url}', error: ${this.error})`}},sl=class extends Gt{urlAfterRedirects;state;type=mt.RoutesRecognized;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`RoutesRecognized(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},_u=class extends Gt{urlAfterRedirects;state;type=mt.GuardsCheckStart;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`GuardsCheckStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},yu=class extends Gt{urlAfterRedirects;state;shouldActivate;type=mt.GuardsCheckEnd;constructor(n,e,i,r,o){super(n,e),this.urlAfterRedirects=i,this.state=r,this.shouldActivate=o}toString(){return`GuardsCheckEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state}, shouldActivate: ${this.shouldActivate})`}},Su=class extends Gt{urlAfterRedirects;state;type=mt.ResolveStart;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Cu=class extends Gt{urlAfterRedirects;state;type=mt.ResolveEnd;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Du=class{route;type=mt.RouteConfigLoadStart;constructor(n){this.route=n}toString(){return`RouteConfigLoadStart(path: ${this.route.path})`}},Eu=class{route;type=mt.RouteConfigLoadEnd;constructor(n){this.route=n}toString(){return`RouteConfigLoadEnd(path: ${this.route.path})`}},xu=class{snapshot;type=mt.ChildActivationStart;constructor(n){this.snapshot=n}toString(){return`ChildActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},wu=class{snapshot;type=mt.ChildActivationEnd;constructor(n){this.snapshot=n}toString(){return`ChildActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Iu=class{snapshot;type=mt.ActivationStart;constructor(n){this.snapshot=n}toString(){return`ActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Mu=class{snapshot;type=mt.ActivationEnd;constructor(n){this.snapshot=n}toString(){return`ActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}};var ls=class{},al=class{},cs=class{url;navigationBehaviorOptions;constructor(n,e){this.url=n,this.navigationBehaviorOptions=e}};function LR(t){return!(t instanceof ls)&&!(t instanceof cs)&&!(t instanceof al)}var Tu=class{rootInjector;outlet=null;route=null;children;attachRef=null;get injector(){return this.route?.snapshot._environmentInjector??this.rootInjector}constructor(n){this.rootInjector=n,this.children=new ro(this.rootInjector)}resetChildren(){this.children=new ro(this.rootInjector)}},ro=(()=>{class t{rootInjector;contexts=new Map;constructor(e){this.rootInjector=e}onChildOutletCreated(e,i){let r=this.getOrCreateContext(e);r.outlet=i,this.contexts.set(e,r)}onChildOutletDestroyed(e){let i=this.getContext(e);i&&(i.outlet=null,i.attachRef=null)}onOutletDeactivated(){let e=this.contexts;return this.contexts=new Map,e}onOutletReAttached(e){this.contexts=e}getOrCreateContext(e){let i=this.getContext(e);return i||(i=new Tu(this.rootInjector),this.contexts.set(e,i)),i}getContext(e){return this.contexts.get(e)||null}static \u0275fac=function(i){return new(i||t)(A(Me))};static \u0275prov=G({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Nu=class{_root;constructor(n){this._root=n}get root(){return this._root.value}parent(n){let e=this.pathFromRoot(n);return e.length>1?e[e.length-2]:null}children(n){let e=xg(n,this._root);return e?e.children.map(i=>i.value):[]}firstChild(n){let e=xg(n,this._root);return e&&e.children.length>0?e.children[0].value:null}siblings(n){let e=wg(n,this._root);return e.length<2?[]:e[e.length-2].children.map(r=>r.value).filter(r=>r!==n)}pathFromRoot(n){return wg(n,this._root).map(e=>e.value)}};function xg(t,n){if(t===n.value)return n;for(let e of n.children){let i=xg(t,e);if(i)return i}return null}function wg(t,n){if(t===n.value)return[n];for(let e of n.children){let i=wg(t,e);if(i.length)return i.unshift(n),i}return[]}var $t=class{value;children;constructor(n,e){this.value=n,this.children=e}toString(){return`TreeNode(${this.value})`}};function rs(t){let n={};return t&&t.children.forEach(e=>n[e.value.outlet]=e),n}var ll=class extends Nu{snapshot;constructor(n,e){super(n),this.snapshot=e,Fg(this,n)}toString(){return this.snapshot.toString()}};function IC(t,n){let e=VR(t,n),i=new tt([new Xi("",{})]),r=new tt({}),o=new tt({}),s=new tt({}),a=new tt(""),l=new Qi(i,r,s,a,o,ae,t,e.root);return l.snapshot=e.root,new ll(new $t(l,[]),e)}function VR(t,n){let e={},i={},r={},s=new ds([],e,r,"",i,ae,t,null,{},n);return new cl("",new $t(s,[]))}var Qi=class{urlSubject;paramsSubject;queryParamsSubject;fragmentSubject;dataSubject;outlet;component;snapshot;_futureSnapshot;_routerState;_paramMap;_queryParamMap;title;url;params;queryParams;fragment;data;resources;_localInjector;pending;paramsSignal;queryParamsSignal;paramMapSignal;queryParamMapSignal;fragmentSignal;dataSignal;constructor(n,e,i,r,o,s,a,l){this.urlSubject=n,this.paramsSubject=e,this.queryParamsSubject=i,this.fragmentSubject=r,this.dataSubject=o,this.outlet=s,this.component=a,this._futureSnapshot=l,this.title=this.dataSubject?.pipe(oe(c=>c[fl]))??Y(void 0),this.url=n,this.params=e,this.queryParams=i,this.fragment=r,this.data=o}get routeConfig(){return this._futureSnapshot.routeConfig}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=this.params.pipe(oe(n=>to(n))),this._paramMap}get queryParamMap(){return this._queryParamMap??=this.queryParams.pipe(oe(n=>to(n))),this._queryParamMap}toString(){return this.snapshot?this.snapshot.toString():`Future(${this._futureSnapshot})`}_setPending(n){this._futureSnapshot=n,this.pending?.set(!0)}},BR="always";function Pg(t,n,e){let i,{routeConfig:r}=t;return n!==null&&(e==="always"||r?.path===""||!n.component&&!n.routeConfig?.loadComponent)?i={params:v(v({},n.params),t.params),data:v(v({},n.data),t.data),resolve:v(v(v(v({},t.data),n.data),r?.data),t._resolvedData)}:i={params:v({},t.params),data:v({},t.data),resolve:v(v({},t.data),t._resolvedData??{})},r&&TC(r)&&(i.resolve[fl]=r.title),i}var ds=class{url;params;queryParams;fragment;data;outlet;component;routeConfig;_resolve;_resolvedData;_routerState;_paramMap;_queryParamMap;_environmentInjector;resources;get title(){return this.data?.[fl]}constructor(n,e,i,r,o,s,a,l,c,d){this.url=n,this.params=e,this.queryParams=i,this.fragment=r,this.data=o,this.outlet=s,this.component=a,this.routeConfig=l,this._resolve=c,this._environmentInjector=d}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=to(this.params),this._paramMap}get queryParamMap(){return this._queryParamMap??=to(this.queryParams),this._queryParamMap}toString(){let n=this.url.map(i=>i.toString()).join("/"),e=this.routeConfig?this.routeConfig.path:"";return`Route(url:'${n}', path:'${e}')`}},cl=class extends Nu{url;constructor(n,e){super(e),this.url=n,Fg(this,e)}toString(){return MC(this._root)}};function Fg(t,n){n.value._routerState=t,n.children.forEach(e=>Fg(t,e))}function MC(t){let n=t.children.length>0?` { ${t.children.map(MC).join(", ")} } `:"";return`${t.value}${n}`}function bg(t){if(t.snapshot){let n=t.snapshot,e=t._futureSnapshot;t.snapshot=e,Wn(n.queryParams,e.queryParams)||t.queryParamsSubject.next(e.queryParams),n.fragment!==e.fragment&&t.fragmentSubject.next(e.fragment),Wn(n.params,e.params)||t.paramsSubject.next(e.params),fR(n.url,e.url)||t.urlSubject.next(e.url),Wn(n.data,e.data)||t.dataSubject.next(e.data)}else t.snapshot=t._futureSnapshot,t.dataSubject.next(t._futureSnapshot.data)}function Ig(t,n){let e=Wn(t.params,n.params)&&vR(t.url,n.url),i=!t.parent!=!n.parent;return e&&!i&&(!t.parent||Ig(t.parent,n.parent))}function TC(t){return typeof t.title=="string"||t.title===null}var NC=new b(""),ml=(()=>{class t{activated=null;get activatedComponentRef(){return this.activated}_activatedRoute=null;name=ae;activateEvents=new W;deactivateEvents=new W;attachEvents=new W;detachEvents=new W;routerOutletData=zt();parentContexts=u(ro);location=u(bt);changeDetector=u(Oe);inputBinder=u(Ou,{optional:!0});supportsBindingToComponentInputs=!0;ngOnChanges(e){if(e.name){let{firstChange:i,previousValue:r}=e.name;if(i)return;this.isTrackedInParentContexts(r)&&(this.deactivate(),this.parentContexts.onChildOutletDestroyed(r)),this.initializeOutletWithName()}}ngOnDestroy(){this.isTrackedInParentContexts(this.name)&&this.parentContexts.onChildOutletDestroyed(this.name),this.inputBinder?.unsubscribeFromRouteData(this)}isTrackedInParentContexts(e){return this.parentContexts.getContext(e)?.outlet===this}ngOnInit(){this.initializeOutletWithName()}initializeOutletWithName(){if(this.parentContexts.onChildOutletCreated(this.name,this),this.activated)return;let e=this.parentContexts.getContext(this.name);e?.route&&(e.attachRef?this.attach(e.attachRef,e.route):this.activateWith(e.route,e.injector))}get isActivated(){return!!this.activated}get component(){if(!this.activated)throw new w(4012,!1);return this.activated.instance}get activatedRoute(){if(!this.activated)throw new w(4012,!1);return this._activatedRoute}get activatedRouteData(){return this._activatedRoute?this._activatedRoute.snapshot.data:{}}detach(){if(!this.activated)throw new w(4012,!1);this.location.detach();let e=this.activated;return this.activated=null,this._activatedRoute=null,this.detachEvents.emit(e.instance),e}attach(e,i){this.activated=e,this._activatedRoute=i,this.location.insert(e.hostView),this.inputBinder?.bindActivatedRouteToOutletComponent(this,this.location.injector),this.attachEvents.emit(e.instance)}deactivate(){if(this.activated){let e=this.component;this.activated.destroy(),this.activated=null,this._activatedRoute=null,this.deactivateEvents.emit(e)}}activateWith(e,i){if(this.isActivated)throw new w(4013,!1);this._activatedRoute=e;let r=this.location,s=e.snapshot.component,a=this.parentContexts.getOrCreateContext(this.name).children,l=new Mg(e,a,r.injector,this.routerOutletData);this.activated=r.createComponent(s,{index:r.length,injector:l,environmentInjector:i}),this.changeDetector.markForCheck(),this.inputBinder?.bindActivatedRouteToOutletComponent(this,this.location.injector),this.activateEvents.emit(this.activated.instance)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=z({type:t,selectors:[["router-outlet"]],inputs:{name:"name",routerOutletData:[1,"routerOutletData"]},outputs:{activateEvents:"activate",deactivateEvents:"deactivate",attachEvents:"attach",detachEvents:"detach"},exportAs:["outlet"],features:[at]})}return t})(),Mg=class{route;childContexts;parent;outletData;constructor(n,e,i,r){this.route=n,this.childContexts=e,this.parent=i,this.outletData=r}get(n,e){return n===Qi?this.route:n===ro?this.childContexts:n===NC?this.outletData:this.parent.get(n,e)}},Ou=new b("");var Lg=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["ng-component"]],exportAs:["emptyRouterOutlet"],decls:1,vars:0,template:function(i,r){i&1&&ie(0,"router-outlet")},dependencies:[ml],encapsulation:2,changeDetection:1})}return t})();function Vg(t){let n=t.children&&t.children.map(Vg),e=n?N(v({},t),{children:n}):v({},t);return!e.component&&!e.loadComponent&&(n||e.loadChildren)&&e.outlet&&e.outlet!==ae&&(e.component=Lg),e}function jR(t,n,e){let i=new Set,r=dl(t,n._root,e?e._root:void 0,i);return{newlyCreatedRoutes:i,state:new ll(r,n)}}function dl(t,n,e,i){if(e&&t.shouldReuseRoute(n.value,e.value.snapshot)){let r=e.value;r._setPending(n.value);let o=HR(t,n,e,i);return new $t(r,o)}else{if(t.shouldAttach(n.value)){let s=t.retrieve(n.value);if(s!==null){let a=s.route;return a.value._setPending(n.value),a.children=n.children.map(l=>dl(t,l,void 0,i)),a}}let r=UR(n.value);r._setPending(n.value),i.add(r);let o=n.children.map(s=>dl(t,s,void 0,i));return new $t(r,o)}}function HR(t,n,e,i){return n.children.map(r=>{for(let o of e.children)if(t.shouldReuseRoute(r.value,o.value.snapshot))return dl(t,r,o,i);return dl(t,r,void 0,i)})}function UR(t){return new Qi(new tt(t.url),new tt(t.params),new tt(t.queryParams),new tt(t.fragment),new tt(t.data),t.outlet,t.component,t)}var us=class{redirectTo;navigationBehaviorOptions;constructor(n,e){this.redirectTo=n,this.navigationBehaviorOptions=e}},kC="ngNavigationCancelingError";function ku(t,n){let{redirectTo:e,navigationBehaviorOptions:i}=as(n)?{redirectTo:n,navigationBehaviorOptions:void 0}:n,r=AC(!1,wt.Redirect);return r.url=e,r.navigationBehaviorOptions=i,r}function AC(t,n){let e=new Error(`NavigationCancelingError: ${t||""}`);return e[kC]=!0,e.cancellationCode=n,e}function zR(t){return RC(t)&&as(t.url)}function RC(t){return!!t&&t[kC]}var Tg=class{routeReuseStrategy;futureState;currState;forwardEvent;inputBindingEnabled;constructor(n,e,i,r,o){this.routeReuseStrategy=n,this.futureState=e,this.currState=i,this.forwardEvent=r,this.inputBindingEnabled=o}activate(n){let e=this.futureState._root,i=this.currState?this.currState._root:null;this.deactivateChildRoutes(e,i,n),bg(this.futureState.root),this.activateChildRoutes(e,i,n)}deactivateChildRoutes(n,e,i){let r=rs(e);n.children.forEach(o=>{let s=o.value.outlet;this.deactivateRoutes(o,r[s],i),delete r[s]}),Object.values(r).forEach(o=>{this.deactivateRouteAndItsChildren(o,i)})}deactivateRoutes(n,e,i){let r=n.value,o=e?e.value:null;if(r===o)if(r.component){let s=i.getContext(r.outlet);s&&this.deactivateChildRoutes(n,e,s.children)}else this.deactivateChildRoutes(n,e,i);else o&&this.deactivateRouteAndItsChildren(e,i)}deactivateRouteAndItsChildren(n,e){n.value.component&&this.routeReuseStrategy.shouldDetach(n.value.snapshot)?this.detachAndStoreRouteSubtree(n,e):this.deactivateRouteAndOutlet(n,e)}detachAndStoreRouteSubtree(n,e){let i=e.getContext(n.value.outlet),r=i&&n.value.component?i.children:e,o=rs(n);for(let s of Object.values(o))this.deactivateRouteAndItsChildren(s,r);if(i&&i.outlet){let s=i.outlet.detach(),a=i.children.contexts;i.resetChildren(),this.routeReuseStrategy.store(n.value.snapshot,{componentRef:s,route:n,contexts:a})}}deactivateRouteAndOutlet(n,e){let i=e.getContext(n.value.outlet),r=i&&n.value.component?i.children:e,o=rs(n);for(let s of Object.values(o))this.deactivateRouteAndItsChildren(s,r);i&&(i.outlet&&(i.outlet.deactivate(),i.children.onOutletDeactivated()),i.attachRef=null,i.route=null),n.value._localInjector?.destroy()}activateChildRoutes(n,e,i){let r=rs(e);n.children.forEach(o=>{this.activateRoutes(o,r[o.value.outlet],i),this.forwardEvent(new Mu(o.value.snapshot))}),n.children.length&&this.forwardEvent(new wu(n.value.snapshot))}activateRoutes(n,e,i){let r=n.value,o=e?e.value:null;if(bg(r),r===o)if(r.component){let s=i.getOrCreateContext(r.outlet);this.activateChildRoutes(n,e,s.children)}else this.activateChildRoutes(n,e,i);else if(r.component){let s=i.getOrCreateContext(r.outlet);if(this.routeReuseStrategy.shouldAttach(r.snapshot)){let a=this.routeReuseStrategy.retrieve(r.snapshot);this.routeReuseStrategy.store(r.snapshot,null),s.children.onOutletReAttached(a.contexts),s.attachRef=a.componentRef,s.route=a.route.value,s.outlet&&s.outlet.attach(a.componentRef,a.route.value),bg(a.route.value),this.activateChildRoutes(n,null,s.children)}else s.attachRef=null,s.route=r,s.outlet&&s.outlet.activateWith(r,s.injector),this.activateChildRoutes(n,null,s.children)}else this.activateChildRoutes(n,null,i)}},Au=class{path;route;constructor(n){this.path=n,this.route=this.path[this.path.length-1]}},ss=class{component;route;constructor(n,e){this.component=n,this.route=e}};function $R(t,n,e){let i=t._root,r=n?n._root:null;return el(i,r,e,[i.value])}function GR(t){let n=t.routeConfig?t.routeConfig.canActivateChild:null;return!n||n.length===0?null:{node:t,guards:n}}function hs(t,n){let e=Symbol(),i=n.get(t,e);return i===e?typeof t=="function"&&!Ih(t)?t:n.get(t):i}function el(t,n,e,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=rs(n);return t.children.forEach(s=>{WR(s,o[s.value.outlet],e,i.concat([s.value]),r),delete o[s.value.outlet]}),Object.entries(o).forEach(([s,a])=>il(a,e.getContext(s),e,r)),r}function WR(t,n,e,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=t.value,s=n?n.value:null,a=e?e.getContext(t.value.outlet):null;if(s&&o.routeConfig===s.routeConfig){let l=qR(s,o,o.routeConfig.runGuardsAndResolvers);l?r.canActivateChecks.push(new Au(i)):(o.data=s.data,o._resolvedData=s._resolvedData),o.component?el(t,n,a?a.children:null,i,r):el(t,n,e,i,r),l&&a&&a.outlet&&a.outlet.isActivated&&r.canDeactivateChecks.push(new ss(a.outlet.component,s))}else s&&il(n,a,e,r),r.canActivateChecks.push(new Au(i)),o.component?el(t,null,a?a.children:null,i,r):el(t,null,e,i,r);return r}function qR(t,n,e){if(typeof e=="function")return ot(n._environmentInjector,()=>e(t,n));switch(e){case"pathParamsChange":return!eo(t.url,n.url);case"pathParamsOrQueryParamsChange":return!eo(t.url,n.url)||!Wn(t.queryParams,n.queryParams);case"always":return!0;case"paramsOrQueryParamsChange":return!Ig(t,n)||!Wn(t.queryParams,n.queryParams);default:return!Ig(t,n)}}function il(t,n,e,i){let r=rs(t),o=t.value;Object.entries(r).forEach(([s,a])=>{o.component?n?il(a,n.children.getContext(s),n.children,i):il(a,null,null,i):il(a,e?e.getContext(s):null,e,i)}),o.component?n&&n.outlet&&n.outlet.isActivated?i.canDeactivateChecks.push(new ss(n.outlet.component,o)):i.canDeactivateChecks.push(new ss(null,o)):i.canDeactivateChecks.push(new ss(null,o))}function pl(t){return typeof t=="function"}function YR(t){return typeof t=="boolean"}function ZR(t){return t&&pl(t.canLoad)}function XR(t){return t&&pl(t.canActivate)}function KR(t){return t&&pl(t.canActivateChild)}function QR(t){return t&&pl(t.canDeactivate)}function JR(t){return t&&pl(t.canMatch)}function OC(t){return t instanceof xr||t?.name==="EmptyError"}var fu=Symbol("INITIAL_VALUE");function fs(){return Ke(t=>ia(t.map(n=>n.pipe(Ae(1),nt(fu)))).pipe(oe(n=>{for(let e of n)if(e!==!0){if(e===fu)return fu;if(e===!1||eO(e))return e}return!0}),ge(n=>n!==fu),Ae(1)))}function eO(t){return as(t)||t instanceof us}function PC(t){return t.aborted?Y(void 0).pipe(Ae(1)):new ne(n=>{let e=()=>{n.next(),n.complete()};return t.addEventListener("abort",e),()=>t.removeEventListener("abort",e)})}function FC(t){return Ce(PC(t))}function tO(t){return Tt(n=>{let{targetSnapshot:e,currentSnapshot:i,guards:{canActivateChecks:r,canDeactivateChecks:o}}=n;return o.length===0&&r.length===0?Y(N(v({},n),{guardsResult:!0})):nO(o,e,i).pipe(Tt(s=>s&&YR(s)?iO(e,r,t):Y(s)),oe(s=>N(v({},n),{guardsResult:s})))})}function nO(t,n,e){return Ve(t).pipe(Tt(i=>lO(i.component,i.route,e,n)),Kn(i=>i!==!0,!0))}function iO(t,n,e){return Ve(n).pipe(Oo(i=>Ai(oO(i.route.parent,e),rO(i.route,e),aO(t,i.path),sO(t,i.route))),Kn(i=>i!==!0,!0))}function rO(t,n){return t!==null&&n&&n(new Iu(t)),Y(!0)}function oO(t,n){return t!==null&&n&&n(new xu(t)),Y(!0)}function sO(t,n){let e=n.routeConfig?n.routeConfig.canActivate:null;if(!e||e.length===0)return Y(!0);let i=e.map(r=>hn(()=>{let o=n._environmentInjector,s=hs(r,o),a=XR(s)?s.canActivate(n,t):ot(o,()=>s(n,t));return oo(a).pipe(Kn())}));return Y(i).pipe(fs())}function aO(t,n){let e=n[n.length-1],r=n.slice(0,n.length-1).reverse().map(o=>GR(o)).filter(o=>o!==null).map(o=>hn(()=>{let s=o.guards.map(a=>{let l=o.node._environmentInjector,c=hs(a,l),d=KR(c)?c.canActivateChild(e,t):ot(l,()=>c(e,t));return oo(d).pipe(Kn())});return Y(s).pipe(fs())}));return Y(r).pipe(fs())}function lO(t,n,e,i){let r=n&&n.routeConfig?n.routeConfig.canDeactivate:null;if(!r||r.length===0)return Y(!0);let o=r.map(s=>{let a=n._environmentInjector,l=hs(s,a),c=QR(l)?l.canDeactivate(t,n,e,i):ot(a,()=>l(t,n,e,i));return oo(c).pipe(Kn())});return Y(o).pipe(fs())}function cO(t,n,e,i,r){let o=n.canLoad;if(o===void 0||o.length===0)return Y(!0);let s=o.map(a=>{let l=hs(a,t),c=ZR(l)?l.canLoad(n,e):ot(t,()=>l(n,e)),d=oo(c);return r?d.pipe(FC(r)):d});return Y(s).pipe(fs(),LC(i))}function LC(t){return uc(dt(n=>{if(typeof n!="boolean")throw ku(t,n)}),oe(n=>n===!0))}function dO(t,n,e,i,r,o){let s=n.canMatch;if(!s||s.length===0)return Y(!0);let a=s.map(l=>{let c=hs(l,t),d=JR(c)?c.canMatch(n,e,r):ot(t,()=>c(n,e,r));return oo(d).pipe(FC(o))});return Y(a).pipe(fs(),LC(i))}var mi=class t extends Error{segmentGroup;constructor(n){super(),this.segmentGroup=n||null,Object.setPrototypeOf(this,t.prototype)}},ul=class t extends Error{urlTree;constructor(n){super(),this.urlTree=n,Object.setPrototypeOf(this,t.prototype)}};function uO(t){throw new w(4e3,!1)}function fO(t){throw AC(!1,wt.GuardRejected)}var Ng=class{urlSerializer;urlTree;constructor(n,e){this.urlSerializer=n,this.urlTree=e}async lineralizeSegments(n,e){let i=[],r=e.root;for(;;){if(i=i.concat(r.segments),r.numberOfChildren===0)return i;if(r.numberOfChildren>1||!r.children[ae])throw uO(`${n.redirectTo}`);r=r.children[ae]}}async applyRedirectCommands(n,e,i,r,o){let s=await hO(e,r,o);if(s instanceof on)throw new ul(s);let a=this.applyRedirectCreateUrlTree(s,this.urlSerializer.parse(s),n,i);if(s[0]==="/")throw new ul(a);return a}applyRedirectCreateUrlTree(n,e,i,r){let o=this.createSegmentGroup(n,e.root,i,r);return new on(o,this.createQueryParams(e.queryParams,this.urlTree.queryParams),e.fragment)}createQueryParams(n,e){let i={};return Object.entries(n).forEach(([r,o])=>{if(typeof o=="string"&&o[0]===":"){let a=o.substring(1);i[r]=e[a]}else i[r]=o}),i}createSegmentGroup(n,e,i,r){let o=this.createSegments(n,e.segments,i,r),s=Object.create(null);return Object.entries(e.children).forEach(([a,l])=>{s[a]=this.createSegmentGroup(n,l,i,r)}),new De(o,s)}createSegments(n,e,i,r){return e.map(o=>o.path[0]===":"?this.findPosParam(n,o,r):this.findOrReturn(o,i))}findPosParam(n,e,i){let r=i[e.path.substring(1)];if(!r)throw new w(4001,!1);return r}findOrReturn(n,e){let i=0;for(let r of e){if(r.path===n.path)return e.splice(i),r;i++}return n}};function hO(t,n,e){if(typeof t=="string")return Promise.resolve(t);let i=t;return gu(oo(ot(e,()=>i(n))))}function mO(t,n){return t.providers&&!t._injector&&(t._injector=ja(t.providers,n,`Route: ${t.path}`)),t._injector??n}function Dn(t){return t.outlet||ae}function pO(t,n){let e=t.filter(i=>Dn(i)===n);return e.push(...t.filter(i=>Dn(i)!==n)),e}var kg={matched:!1,consumedSegments:[],remainingSegments:[],parameters:{},positionalParamSegments:{}};function VC(t){return{routeConfig:t.routeConfig,url:t.url,params:t.params,queryParams:t.queryParams,fragment:t.fragment,data:t.data,outlet:t.outlet,title:t.title,paramMap:t.paramMap,queryParamMap:t.queryParamMap}}function gO(t,n,e,i,r,o,s){let a=BC(t,n,e);if(!a.matched)return Y(a);let l=VC(o(a));return i=mO(n,i),dO(i,n,e,r,l,s).pipe(oe(c=>c===!0?a:v({},kg)))}function BC(t,n,e){if(n.path==="")return n.pathMatch==="full"&&(t.hasChildren()||e.length>0)?v({},kg):{matched:!0,consumedSegments:[],remainingSegments:e,parameters:{},positionalParamSegments:{}};let r=(n.matcher||cC)(e,t,n);if(!r)return v({},kg);let o={};Object.entries(r.posParams??{}).forEach(([a,l])=>{o[a]=l.path});let s=r.consumed.length>0?v(v({},o),r.consumed[r.consumed.length-1].parameters):o;return{matched:!0,consumedSegments:r.consumed,remainingSegments:e.slice(r.consumed.length),parameters:s,positionalParamSegments:r.posParams??{}}}function aC(t,n,e,i,r){return e.length>0&&_O(t,e,i,r)?{segmentGroup:new De(n,bO(i,new De(e,t.children))),slicedSegments:[]}:e.length===0&&yO(t,e,i)?{segmentGroup:new De(t.segments,vO(t,e,i,t.children)),slicedSegments:e}:{segmentGroup:new De(t.segments,t.children),slicedSegments:e}}function vO(t,n,e,i){let r={};for(let o of e)if(Pu(t,n,o)&&!i[Dn(o)]){let s=new De([],{});r[Dn(o)]=s}return v(v({},i),r)}function bO(t,n){let e={};e[ae]=n;for(let i of t)if(i.path===""&&Dn(i)!==ae){let r=new De([],{});e[Dn(i)]=r}return e}function _O(t,n,e,i){return e.some(r=>!Pu(t,n,r)||!(Dn(r)!==ae)?!1:!(i!==void 0&&Dn(r)===i))}function yO(t,n,e){return e.some(i=>Pu(t,n,i))}function Pu(t,n,e){return(t.hasChildren()||n.length>0)&&e.pathMatch==="full"?!1:e.path===""}function SO(t,n,e){return n.length===0&&!t.children[e]}var Ag=class{};async function CO(t,n,e,i,r,o,s,a){return new Rg(t,n,e,i,r,s,o,a).recognize()}var DO=31,Rg=class{injector;configLoader;rootComponentType;config;urlTree;paramsInheritanceStrategy;urlSerializer;abortSignal;applyRedirects;absoluteRedirectCount=0;allowRedirects=!0;constructor(n,e,i,r,o,s,a,l){this.injector=n,this.configLoader=e,this.rootComponentType=i,this.config=r,this.urlTree=o,this.paramsInheritanceStrategy=s,this.urlSerializer=a,this.abortSignal=l,this.applyRedirects=new Ng(this.urlSerializer,this.urlTree)}noMatchError(n){return new w(4002,`'${n.segmentGroup}'`)}async recognize(){let n=aC(this.urlTree.root,[],[],this.config).segmentGroup,{children:e,rootSnapshot:i}=await this.match(n),r=new $t(i,e),o=new cl("",r),s=SC(i,[],this.urlTree.queryParams,this.urlTree.fragment);return s.queryParams=this.urlTree.queryParams,o.url=this.urlSerializer.serialize(s),{state:o,tree:s}}async match(n){let e=new ds([],Object.freeze({}),Object.freeze(v({},this.urlTree.queryParams)),this.urlTree.fragment,Object.freeze({}),ae,this.rootComponentType,null,{},this.injector);try{return{children:await this.processSegmentGroup(this.injector,this.config,n,ae,e),rootSnapshot:e}}catch(i){if(i instanceof ul)return this.urlTree=i.urlTree,this.match(i.urlTree.root);throw i instanceof mi?this.noMatchError(i):i}}async processSegmentGroup(n,e,i,r,o){if(i.segments.length===0&&i.hasChildren())return this.processChildren(n,e,i,o);let s=await this.processSegment(n,e,i,i.segments,r,!0,o);return s instanceof $t?[s]:[]}async processChildren(n,e,i,r){let o=[];for(let l of Object.keys(i.children))l==="primary"?o.unshift(l):o.push(l);let s=[];for(let l of o){let c=i.children[l],d=pO(e,l),f=await this.processSegmentGroup(n,d,c,l,r);s.push(...f)}let a=jC(s);return EO(a),a}async processSegment(n,e,i,r,o,s,a){for(let l of e)try{return await this.processSegmentAgainstRoute(l._injector??n,e,l,i,r,o,s,a)}catch(c){if(c instanceof mi||OC(c))continue;throw c}if(SO(i,r,o))return new Ag;throw new mi(i)}async processSegmentAgainstRoute(n,e,i,r,o,s,a,l){if(Dn(i)!==s&&(s===ae||!Pu(r,o,i)))throw new mi(r);if(i.redirectTo===void 0)return this.matchSegmentAgainstRoute(n,r,i,o,s,l);if(this.allowRedirects&&a)return this.expandSegmentAgainstRouteUsingRedirect(n,r,e,i,o,s,l);throw new mi(r)}async expandSegmentAgainstRouteUsingRedirect(n,e,i,r,o,s,a){let{matched:l,parameters:c,consumedSegments:d,positionalParamSegments:f,remainingSegments:h}=BC(e,r,o);if(!l)throw new mi(e);typeof r.redirectTo=="string"&&r.redirectTo[0]==="/"&&(this.absoluteRedirectCount++,this.absoluteRedirectCount>DO&&(this.allowRedirects=!1));let p=this.createSnapshot(n,r,o,c,a);if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let S=await this.applyRedirects.applyRedirectCommands(d,r.redirectTo,f,VC(p),n),E=await this.applyRedirects.lineralizeSegments(r,S);return this.processSegment(n,i,e,E.concat(h),s,!1,a)}createSnapshot(n,e,i,r,o){let s=new ds(i,r,Object.freeze(v({},this.urlTree.queryParams)),this.urlTree.fragment,wO(e),Dn(e),e.component??e._loadedComponent??null,e,IO(e),n),a=Pg(s,o,this.paramsInheritanceStrategy);return s.params=Object.freeze(a.params),s.data=Object.freeze(a.data),s}async matchSegmentAgainstRoute(n,e,i,r,o,s){if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let a=Ge=>this.createSnapshot(n,i,Ge.consumedSegments,Ge.parameters,s),l=await gu(gO(e,i,r,n,this.urlSerializer,a,this.abortSignal));if(i.path==="**"&&(e.children={}),!l?.matched)throw new mi(e);n=i._injector??n;let{routes:c}=await this.getChildConfig(n,i,r),d=i._loadedInjector??n,{parameters:f,consumedSegments:h,remainingSegments:p}=l,S=this.createSnapshot(n,i,h,f,s),{segmentGroup:E,slicedSegments:$}=aC(e,h,p,c,o);if($.length===0&&E.hasChildren()){let Ge=await this.processChildren(d,c,E,S);return new $t(S,Ge)}if(c.length===0&&$.length===0)return new $t(S,[]);let H=Dn(i)===o,Se=await this.processSegment(d,c,E,$,H?ae:o,!0,S);return new $t(S,Se instanceof $t?[Se]:[])}async getChildConfig(n,e,i){if(e.children)return{routes:e.children,injector:n};if(e.loadChildren){if(e._loadedRoutes!==void 0){let o=e._loadedNgModuleFactory;return o&&!e._loadedInjector&&(e._loadedInjector=o.create(n).injector),{routes:e._loadedRoutes,injector:e._loadedInjector}}if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);if(await gu(cO(n,e,i,this.urlSerializer,this.abortSignal))){let o=await this.configLoader.loadChildren(n,e);return e._loadedRoutes=o.routes,e._loadedInjector=o.injector,e._loadedNgModuleFactory=o.factory,o}throw fO(e)}return{routes:[],injector:n}}};function EO(t){t.sort((n,e)=>n.value.outlet===ae?-1:e.value.outlet===ae?1:n.value.outlet.localeCompare(e.value.outlet))}function xO(t){let n=t.value.routeConfig;return n&&n.path===""}function jC(t){let n=[],e=new Set;for(let i of t){if(!xO(i)){n.push(i);continue}let r=n.find(o=>i.value.routeConfig===o.value.routeConfig);r!==void 0?(r.children.push(...i.children),e.add(r)):n.push(i)}for(let i of e){let r=jC(i.children);n.push(new $t(i.value,r))}return n.filter(i=>!e.has(i))}function wO(t){return t.data||{}}function IO(t){return t.resolve||{}}function MO(t,n,e,i,r,o,s){return Tt(async a=>{let{state:l,tree:c}=await CO(t,n,e,i,a.extractedUrl,r,o,s);return N(v({},a),{targetSnapshot:l,urlAfterRedirects:c})})}function TO(t){return Tt(n=>{let{targetSnapshot:e,guards:{canActivateChecks:i}}=n;if(!i.length)return Y(n);let r=new Set(i.map(a=>a.route)),o=new Set;for(let a of r)if(!o.has(a))for(let l of HC(a))o.add(l);let s=0;return Ve(o).pipe(Oo(a=>r.has(a)?NO(a,e,t):(a.data=Pg(a,a.parent,t).resolve,Y(void 0))),dt(()=>s++),Fc(1),Tt(a=>s===o.size?Y(n):We))})}function HC(t){let n=t.children.map(e=>HC(e)).flat();return[t,...n]}function NO(t,n,e){let i=t.routeConfig,r=t._resolve;return i?.title!==void 0&&!TC(i)&&(r[fl]=i.title),hn(()=>(t.data=Pg(t,t.parent,e).resolve,kO(r,t,n).pipe(oe(o=>(t._resolvedData=o,t.data=v(v({},t.data),o),null)))))}function kO(t,n,e){let i=yg(t);if(i.length===0)return Y({});let r={};return Ve(i).pipe(Tt(o=>AO(t[o],n,e).pipe(Kn(),dt(s=>{if(s instanceof us)throw ku(new Ki,s);r[o]=s}))),Fc(1),oe(()=>r),wr(o=>OC(o)?We:ta(o)))}function AO(t,n,e){let i=n._environmentInjector,r=hs(t,i),o=r.resolve?r.resolve(n,e):ot(i,()=>r(n,e));return oo(o)}var UC=new b("");function Og(t){return Ke(n=>{let e=t(n);return e?Ve(e).pipe(oe(()=>n)):Y(n)})}var Bg=(()=>{class t{buildTitle(e){let i,r=e.root;for(;r!==void 0;)i=this.getResolvedTitleForRoute(r)??i,r=r.children.find(o=>o.outlet===ae);return i}getResolvedTitleForRoute(e){return e.data[fl]}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:()=>u(zC)})}return t})(),zC=(()=>{class t extends Bg{title;constructor(e){super(),this.title=e}updateTitle(e){let i=this.buildTitle(e);i!==void 0&&this.title.setTitle(i)}static \u0275fac=function(i){return new(i||t)(A(eC))};static \u0275prov=G({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),gl=new b("",{factory:()=>({})}),vl=new b(""),$C=(()=>{class t{componentLoaders=new WeakMap;childrenLoaders=new WeakMap;onLoadStartListener;onLoadEndListener;compiler=u(Gp);async loadComponent(e,i){if(this.componentLoaders.get(i))return this.componentLoaders.get(i);if(i._loadedComponent)return Promise.resolve(i._loadedComponent);this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let o=await uC(ot(e,()=>i.loadComponent())),s=await WC(Zp(o));return this.onLoadEndListener&&this.onLoadEndListener(i),i._loadedComponent=s,s}finally{this.componentLoaders.delete(i)}})();return this.componentLoaders.set(i,r),r}loadChildren(e,i){if(this.childrenLoaders.get(i))return this.childrenLoaders.get(i);if(i._loadedRoutes)return Promise.resolve({routes:i._loadedRoutes,injector:i._loadedInjector});this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let o=await GC(i,this.compiler,e,this.onLoadEndListener);return i._loadedRoutes=o.routes,i._loadedInjector=o.injector,i._loadedNgModuleFactory=o.factory,o}finally{this.childrenLoaders.delete(i)}})();return this.childrenLoaders.set(i,r),r}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();async function GC(t,n,e,i){let r=await uC(ot(e,()=>t.loadChildren())),o=await WC(Zp(r)),s;o instanceof Xd||Array.isArray(o)?s=o:s=await n.compileModuleAsync(o),i&&i(t);let a,l,c=!1,d;return Array.isArray(s)?(l=s,c=!0):(a=s.create(e).injector,d=s,l=a.get(vl,[],{optional:!0,self:!0}).flat()),{routes:l.map(Vg),injector:a,factory:d}}async function WC(t){return t}var Fu=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:()=>u(RO)})}return t})(),RO=(()=>{class t{shouldProcessUrl(e){return!0}extract(e){return e}merge(e,i){return e}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})(),qC=new b("");var OO=()=>{},YC=new b(""),ZC=(()=>{class t{currentNavigation=U(null,{equal:()=>!1});currentTransition=null;lastSuccessfulNavigation=U(null);events=new x;transitionAbortWithErrorSubject=new x;configLoader=u($C);environmentInjector=u(Me);destroyRef=u(qe);urlSerializer=u(hl);rootContexts=u(ro);location=u(Zi);inputBindingEnabled=u(Ou,{optional:!0})!==null;titleStrategy=u(Bg);options=u(gl,{optional:!0})||{};paramsInheritanceStrategy=this.options.paramsInheritanceStrategy||BR;urlHandlingStrategy=u(Fu);createViewTransition=u(qC,{optional:!0});navigationErrorHandler=u(YC,{optional:!0});routerResourcesFeature=u(UC,{optional:!0});navigationId=0;get hasRequestedNavigation(){return this.navigationId!==0}transitions;afterPreactivation=()=>Y(void 0);rootComponentType=null;destroyed=!1;constructor(){let e=r=>this.events.next(new Du(r)),i=r=>this.events.next(new Eu(r));this.configLoader.onLoadEndListener=i,this.configLoader.onLoadStartListener=e,this.destroyRef.onDestroy(()=>{this.destroyed=!0})}complete(){this.transitions?.complete()}handleNavigationRequest(e){let i=++this.navigationId;Ne(()=>{this.transitions?.next(N(v({},e),{extractedUrl:this.urlHandlingStrategy.extract(e.rawUrl),targetSnapshot:null,targetRouterState:null,guards:{canActivateChecks:[],canDeactivateChecks:[]},guardsResult:null,id:i,routesRecognizeHandler:{},beforeActivateHandler:{}}))})}setupNavigations(e){return this.transitions=new tt(null),this.transitions.pipe(ge(i=>i!==null),Ke(i=>{let r=!0,o=!1,s=new AbortController,a=()=>!o&&this.currentTransition?.id===i.id;return Y(i).pipe(Ke(l=>{if(this.navigationId>i.id)return this.cancelNavigationTransition(i,"",wt.SupersededByNewNavigation),We;this.currentTransition=i;let c=this.lastSuccessfulNavigation();this.currentNavigation.set({id:l.id,initialUrl:l.rawUrl,extractedUrl:l.extractedUrl,targetBrowserUrl:typeof l.extras.browserUrl=="string"?this.urlSerializer.parse(l.extras.browserUrl):l.extras.browserUrl,trigger:l.source,extras:l.extras,previousNavigation:c?N(v({},c),{previousNavigation:null}):null,abort:()=>s.abort(),routesRecognizeHandler:l.routesRecognizeHandler,beforeActivateHandler:l.beforeActivateHandler});let d=!e.navigated||this.isUpdatingInternalState()||this.isUpdatedBrowserUrl(),f=l.extras.onSameUrlNavigation??e.onSameUrlNavigation;if(!d&&f!=="reload")return this.events.next(new gi(l.id,this.urlSerializer.serialize(l.rawUrl),"",ol.IgnoredSameUrlNavigation)),l.resolve(!1),We;if(this.urlHandlingStrategy.shouldProcessUrl(l.rawUrl))return Y(l).pipe(Ke(h=>(this.events.next(new no(h.id,this.urlSerializer.serialize(h.extractedUrl),h.source,h.restoredState)),h.id!==this.navigationId?We:Promise.resolve(h))),MO(this.environmentInjector,this.configLoader,this.rootComponentType,e.config,this.urlSerializer,this.paramsInheritanceStrategy,s.signal),dt(h=>{i.targetSnapshot=h.targetSnapshot,i.urlAfterRedirects=h.urlAfterRedirects,this.currentNavigation.update(p=>(p.finalUrl=h.urlAfterRedirects,p)),this.events.next(new al)}),Ke(h=>Ve(i.routesRecognizeHandler.deferredHandle??Y(void 0)).pipe(oe(()=>h))),dt(()=>{let h=new sl(l.id,this.urlSerializer.serialize(l.extractedUrl),this.urlSerializer.serialize(l.urlAfterRedirects),l.targetSnapshot);this.events.next(h)}));if(d&&this.urlHandlingStrategy.shouldProcessUrl(l.currentRawUrl)){let{id:h,extractedUrl:p,source:S,restoredState:E,extras:$}=l,H=new no(h,this.urlSerializer.serialize(p),S,E);this.events.next(H);let Se=IC(this.rootComponentType,this.environmentInjector).snapshot;return this.currentTransition=i=N(v({},l),{targetSnapshot:Se,urlAfterRedirects:p,extras:N(v({},$),{skipLocationChange:!1,replaceUrl:!1})}),this.currentNavigation.update(Ge=>(Ge.finalUrl=p,Ge)),Y(i)}else return this.events.next(new gi(l.id,this.urlSerializer.serialize(l.extractedUrl),"",ol.IgnoredByUrlHandlingStrategy)),l.resolve(!1),We}),oe(l=>{let c=new _u(l.id,this.urlSerializer.serialize(l.extractedUrl),this.urlSerializer.serialize(l.urlAfterRedirects),l.targetSnapshot);return this.events.next(c),this.currentTransition=i=N(v({},l),{guards:$R(l.targetSnapshot,l.currentSnapshot,this.rootContexts)}),i}),tO(l=>this.events.next(l)),Ke(l=>{if(i.guardsResult=l.guardsResult,l.guardsResult&&typeof l.guardsResult!="boolean")throw ku(this.urlSerializer,l.guardsResult);let c=new yu(l.id,this.urlSerializer.serialize(l.extractedUrl),this.urlSerializer.serialize(l.urlAfterRedirects),l.targetSnapshot,!!l.guardsResult);if(this.events.next(c),!a())return We;if(!l.guardsResult)return this.cancelNavigationTransition(l,"",wt.GuardRejected),We;if(l.guards.canActivateChecks.length===0)return Y(l);let d=new Su(l.id,this.urlSerializer.serialize(l.extractedUrl),this.urlSerializer.serialize(l.urlAfterRedirects),l.targetSnapshot);if(this.events.next(d),!a())return We;let f=!1;return Y(l).pipe(TO(this.paramsInheritanceStrategy),dt({next:()=>{f=!0;let h=new Cu(l.id,this.urlSerializer.serialize(l.extractedUrl),this.urlSerializer.serialize(l.urlAfterRedirects),l.targetSnapshot);this.events.next(h)},complete:()=>{f||this.cancelNavigationTransition(l,"",wt.NoDataFromResolver)}}))}),Og(l=>{let c=f=>{let h=[];if(f.routeConfig?._loadedComponent)f.component=f.routeConfig?._loadedComponent;else if(f.routeConfig?.loadComponent){let p=f._environmentInjector;h.push(this.configLoader.loadComponent(p,f.routeConfig).then(S=>{f.component=S}))}for(let p of f.children)h.push(...c(p));return h},d=c(l.targetSnapshot.root);return d.length===0?Y(l):Ve(Promise.all(d).then(()=>l))}),Ke(l=>{let{newlyCreatedRoutes:c,state:d}=jR(e.routeReuseStrategy,l.targetSnapshot,l.currentRouterState);return this.currentTransition=i=l=N(v({},l),{targetRouterState:d,newlyCreatedRoutes:c}),this.currentNavigation.update(f=>(f.targetRouterState=d,f)),Y(l)}),this.routerResourcesFeature?.setupAndRunResources(s.signal)??(l=>l),Og(()=>this.afterPreactivation()),Ke(()=>{let{currentSnapshot:l,targetSnapshot:c}=i,d=this.createViewTransition?.(this.environmentInjector,l.root,c.root,i.hasUAVisualTransition);return d?Ve(d).pipe(oe(()=>i)):Y(i)}),Ae(1),Ke(l=>{r=!1,this.events.next(new ls);let c=i.beforeActivateHandler.deferredHandle;return c?Ve(c.then(()=>l)):Y(l)}),dt(l=>{new Tg(e.routeReuseStrategy,i.targetRouterState,i.currentRouterState,c=>this.events.next(c),this.inputBindingEnabled).activate(this.rootContexts),l.newlyCreatedRoutes?.clear(),a()&&(XC(l.targetRouterState),o=!0,this.currentNavigation.update(c=>(c.abort=OO,c)),this.lastSuccessfulNavigation.set(Ne(this.currentNavigation)),this.events.next(new pi(l.id,this.urlSerializer.serialize(l.extractedUrl),this.urlSerializer.serialize(l.urlAfterRedirects))),this.titleStrategy?.updateTitle(l.targetRouterState.snapshot),l.resolve(!0))}),Ce(PC(s.signal).pipe(ge(()=>!o&&r),dt(()=>{this.cancelNavigationTransition(i,s.signal.reason+"",wt.Aborted)}))),dt({complete:()=>{o=!0}}),Ce(this.transitionAbortWithErrorSubject.pipe(dt(l=>{throw l}))),Ir(()=>{s.abort(),o||this.cancelNavigationTransition(i,"",wt.SupersededByNewNavigation),this.currentTransition?.id===i.id&&(this.currentNavigation.set(null),this.currentTransition=null)}),wr(l=>{if(o=!0,lC(i),this.destroyed)return i.resolve(!1),We;if(RC(l))this.events.next(new rn(i.id,this.urlSerializer.serialize(i.extractedUrl),l.message,l.cancellationCode)),zR(l)?this.events.next(new cs(l.url,l.navigationBehaviorOptions)):i.resolve(!1);else{let c=new io(i.id,this.urlSerializer.serialize(i.extractedUrl),l,i.targetSnapshot??void 0);try{let d=ot(this.environmentInjector,()=>this.navigationErrorHandler?.(c));if(d instanceof us){let{message:f,cancellationCode:h}=ku(this.urlSerializer,d);this.events.next(new rn(i.id,this.urlSerializer.serialize(i.extractedUrl),f,h)),this.events.next(new cs(d.redirectTo,d.navigationBehaviorOptions))}else throw this.events.next(c),l}catch(d){this.options.resolveNavigationPromiseOnError?i.resolve(!1):i.reject(d)}}return We}))}))}cancelNavigationTransition(e,i,r){lC(e);let o=new rn(e.id,this.urlSerializer.serialize(e.extractedUrl),i,r);this.events.next(o),e.resolve(!1)}isUpdatingInternalState(){return this.currentTransition?.extractedUrl.toString()!==this.currentTransition?.currentUrlTree.toString()}isUpdatedBrowserUrl(){let e=this.urlHandlingStrategy.extract(this.urlSerializer.parse(this.location.path(!0))),i=Ne(this.currentNavigation),r=i?.targetBrowserUrl??i?.extractedUrl;return e.toString()!==r?.toString()&&!i?.extras.skipLocationChange}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();function PO(t){return t!==nl}function lC(t){for(let n of t.newlyCreatedRoutes??[])n._localInjector?.destroy(),n._localInjector=void 0;XC(t.targetRouterState)}function XC(t){if(!t)return;let n=e=>{e.value.pending?.set(!1),e.children.forEach(n)};n(t._root)}var KC=new b("");var QC=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:()=>u(FO)})}return t})(),Ru=class{shouldDetach(n){return!1}store(n,e){}shouldAttach(n){return!1}retrieve(n){return null}shouldReuseRoute(n,e){return n.routeConfig===e.routeConfig}shouldDestroyInjector(n){return!0}},FO=(()=>{class t extends Ru{static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})(),jg=(()=>{class t{urlSerializer=u(hl);options=u(gl,{optional:!0})||{};canceledNavigationResolution=this.options.canceledNavigationResolution||"replace";location=u(Zi);urlHandlingStrategy=u(Fu);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";currentUrlTree=new on;getCurrentUrlTree(){return this.currentUrlTree}rawUrlTree=this.currentUrlTree;getRawUrlTree(){return this.rawUrlTree}createBrowserPath({finalUrl:e,initialUrl:i,targetBrowserUrl:r}){let o=e!==void 0?this.urlHandlingStrategy.merge(e,i):i,s=r??o;return s instanceof on?this.urlSerializer.serialize(s):s}routerUrlState(e){return e?.targetBrowserUrl===void 0||e?.finalUrl===void 0?{}:{\u0275routerUrl:this.urlSerializer.serialize(e.finalUrl)}}commitTransition({targetRouterState:e,finalUrl:i,initialUrl:r}){i&&e?(this.currentUrlTree=i,this.rawUrlTree=this.urlHandlingStrategy.merge(i,r),this.routerState=e):this.rawUrlTree=r}routerState=IC(null,u(Me));getRouterState(){return this.routerState}_stateMemento=this.createStateMemento();get stateMemento(){return this._stateMemento}updateStateMemento(){this._stateMemento=this.createStateMemento()}createStateMemento(){return{rawUrlTree:this.rawUrlTree,currentUrlTree:this.currentUrlTree,routerState:this.routerState}}restoredState(){return this.location.getState()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:()=>u(LO)})}return t})(),LO=(()=>{class t extends jg{currentPageId=0;lastSuccessfulId=-1;get browserPageId(){return this.canceledNavigationResolution!=="computed"?this.currentPageId:this.restoredState()?.\u0275routerPageId??this.currentPageId}registerNonRouterCurrentEntryChangeListener(e){return this.location.subscribe(i=>{i.type==="popstate"&&setTimeout(()=>{e(i.url,i.state,"popstate",{replaceUrl:!0},i.hasUAVisualTransition)})})}handleRouterEvent(e,i){e instanceof no?this.updateStateMemento():e instanceof gi?this.commitTransition(i):e instanceof sl?this.urlUpdateStrategy==="eager"&&(i.extras.skipLocationChange||this.setBrowserUrl(this.createBrowserPath(i),i)):e instanceof ls?(this.commitTransition(i),this.urlUpdateStrategy==="deferred"&&!i.extras.skipLocationChange&&this.setBrowserUrl(this.createBrowserPath(i),i)):e instanceof rn&&!wC(e)?this.restoreHistory(i):e instanceof io?this.restoreHistory(i,!0):e instanceof pi&&(this.lastSuccessfulId=e.id,this.currentPageId=this.browserPageId)}setBrowserUrl(e,i){let{extras:r,id:o}=i,{replaceUrl:s,state:a}=r;if(this.location.isCurrentPathEqualTo(e)||s){let l=this.browserPageId,c=v(v({},a),this.generateNgRouterState(o,l,i));this.location.replaceState(e,"",c)}else{let l=v(v({},a),this.generateNgRouterState(o,this.browserPageId+1,i));this.location.go(e,"",l)}}restoreHistory(e,i=!1){if(this.canceledNavigationResolution==="computed"){let r=this.browserPageId,o=this.currentPageId-r;o!==0?this.location.historyGo(o):this.getCurrentUrlTree()===e.finalUrl&&o===0&&(this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}else this.canceledNavigationResolution==="replace"&&(i&&this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}resetInternalState({finalUrl:e}){this.routerState=this.stateMemento.routerState,this.currentUrlTree=this.stateMemento.currentUrlTree,this.rawUrlTree=this.urlHandlingStrategy.merge(this.currentUrlTree,e??this.rawUrlTree)}resetUrlToCurrentUrlTree(){this.location.replaceState(this.urlSerializer.serialize(this.getRawUrlTree()),"",this.generateNgRouterState(this.lastSuccessfulId,this.currentPageId))}generateNgRouterState(e,i,r){return this.canceledNavigationResolution==="computed"?v({navigationId:e,\u0275routerPageId:i},this.routerUrlState(r)):v({navigationId:e},this.routerUrlState(r))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();function Hg(t,n){t.events.pipe(ge(e=>e instanceof pi||e instanceof rn||e instanceof io||e instanceof gi),oe(e=>e instanceof pi||e instanceof gi?0:(e instanceof rn?e.code===wt.Redirect||e.code===wt.SupersededByNewNavigation:!1)?2:1),ge(e=>e!==2),Ae(1)).subscribe(()=>{n()})}var Lu=(()=>{class t{get currentUrlTree(){return this.stateManager.getCurrentUrlTree()}get rawUrlTree(){return this.stateManager.getRawUrlTree()}disposed=!1;nonRouterCurrentEntryChangeSubscription;console=u(Qd);stateManager=u(jg);options=u(gl,{optional:!0})||{};pendingTasks=u(oi);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";navigationTransitions=u(ZC);urlSerializer=u(hl);location=u(Zi);urlHandlingStrategy=u(Fu);injector=u(Me);_events=new x;get events(){return this._events}get routerState(){return this.stateManager.getRouterState()}navigated=!1;routeReuseStrategy=u(QC);injectorCleanup=u(KC,{optional:!0});onSameUrlNavigation=this.options.onSameUrlNavigation||"ignore";config=u(vl,{optional:!0})?.flat()??[];componentInputBindingEnabled=!!u(Ou,{optional:!0});currentNavigation=this.navigationTransitions.currentNavigation.asReadonly();constructor(){this.resetConfig(this.config),this.navigationTransitions.setupNavigations(this).subscribe({error:e=>{}}),this.subscribeToNavigationEvents()}eventsSubscription=new pe;subscribeToNavigationEvents(){let e=this.navigationTransitions.events.subscribe(i=>{try{let r=this.navigationTransitions.currentTransition,o=Ne(this.navigationTransitions.currentNavigation);if(r!==null&&o!==null){if(this.stateManager.handleRouterEvent(i,o),i instanceof rn&&i.code!==wt.Redirect&&i.code!==wt.SupersededByNewNavigation)this.navigated=!0;else if(i instanceof pi)this.navigated=!0,this.injectorCleanup?.(this.routeReuseStrategy,this.routerState,this.config);else if(i instanceof cs){let s=i.navigationBehaviorOptions,a=this.urlHandlingStrategy.merge(i.url,r.currentRawUrl),l=v({scroll:r.extras.scroll,browserUrl:r.extras.browserUrl,info:r.extras.info,skipLocationChange:r.extras.skipLocationChange,replaceUrl:r.extras.replaceUrl||this.urlUpdateStrategy==="eager"||PO(r.source)},s);this.scheduleNavigation(a,nl,null,l,r.hasUAVisualTransition,{resolve:r.resolve,reject:r.reject,promise:r.promise})}}LR(i)&&this._events.next(i)}catch(r){this.navigationTransitions.transitionAbortWithErrorSubject.next(r)}});this.eventsSubscription.add(e)}resetRootComponentType(e){this.routerState.root.component=e,this.navigationTransitions.rootComponentType=e}initialNavigation(){this.setUpLocationChangeListener(),this.navigationTransitions.hasRequestedNavigation||this.navigateToSyncWithBrowser(this.location.path(!0),nl,this.stateManager.restoredState(),{replaceUrl:!0})}setUpLocationChangeListener(){this.nonRouterCurrentEntryChangeSubscription??=this.stateManager.registerNonRouterCurrentEntryChangeListener((e,i,r,o,s)=>{this.navigateToSyncWithBrowser(e,r,i,o,s)})}navigateToSyncWithBrowser(e,i,r,o,s){let a=r?.navigationId?r:null,l=r?.\u0275routerUrl??e;if(r?.\u0275routerUrl&&(o=N(v({},o),{browserUrl:e})),r){let d=v({},r);delete d.navigationId,delete d.\u0275routerPageId,delete d.\u0275routerUrl,Object.keys(d).length!==0&&(o.state=d)}let c=this.parseUrl(l);this.scheduleNavigation(c,i,a,o,s).catch(d=>{this.disposed||this.injector.get(Xt)(d)})}get url(){return this.serializeUrl(this.currentUrlTree)}getCurrentNavigation(){return Ne(this.navigationTransitions.currentNavigation)}get lastSuccessfulNavigation(){return this.navigationTransitions.lastSuccessfulNavigation}resetConfig(e){this.config=e.map(Vg),this.navigated=!1}ngOnDestroy(){this.dispose()}dispose(){this._events.unsubscribe(),this.navigationTransitions.complete(),this.nonRouterCurrentEntryChangeSubscription?.unsubscribe(),this.nonRouterCurrentEntryChangeSubscription=void 0,this.disposed=!0,this.eventsSubscription.unsubscribe()}createUrlTree(e,i={}){let{relativeTo:r,queryParams:o,fragment:s,queryParamsHandling:a,preserveFragment:l}=i,c=l?this.currentUrlTree.fragment:s,d=null;switch(a??this.options.defaultQueryParamsHandling){case"merge":d=v(v({},this.currentUrlTree.queryParams),o);break;case"preserve":d=this.currentUrlTree.queryParams;break;default:d=o||null}d!==null&&(d=this.removeEmptyProps(d));let f;try{let h=r?r.snapshot:this.routerState.snapshot.root;f=CC(h)}catch{(typeof e[0]!="string"||e[0][0]!=="/")&&(e=[]),f=this.currentUrlTree.root}return DC(f,e,d,c??null,this.urlSerializer)}navigateByUrl(e,i={skipLocationChange:!1}){let r=as(e)?e:this.parseUrl(e),o=this.urlHandlingStrategy.merge(r,this.rawUrlTree);return this.scheduleNavigation(o,nl,null,i)}navigate(e,i={skipLocationChange:!1}){return VO(e),this.navigateByUrl(this.createUrlTree(e,i),i)}serializeUrl(e){return this.urlSerializer.serialize(e)}parseUrl(e){try{return this.urlSerializer.parse(e)}catch{return this.console.warn(ti(4018,!1)),this.urlSerializer.parse("/")}}isActive(e,i){let r;if(i===!0?r=v({},hC):i===!1?r=v({},Sg):r=v(v({},Sg),i),as(e))return nC(this.currentUrlTree,e,r);let o=this.parseUrl(e);return nC(this.currentUrlTree,o,r)}removeEmptyProps(e){return Object.entries(e).reduce((i,[r,o])=>(o!=null&&(i[r]=o),i),{})}scheduleNavigation(e,i,r,o,s,a){if(this.disposed)return Promise.resolve(!1);let l,c,d;a?(l=a.resolve,c=a.reject,d=a.promise):d=new Promise((h,p)=>{l=h,c=p});let f=this.pendingTasks.add();return Hg(this,()=>{queueMicrotask(()=>this.pendingTasks.remove(f))}),this.navigationTransitions.handleNavigationRequest({source:i,restoredState:r,currentUrlTree:this.currentUrlTree,currentRawUrl:this.currentUrlTree,rawUrl:e,extras:o,hasUAVisualTransition:s,resolve:l,reject:c,promise:d,currentSnapshot:this.routerState.snapshot,currentRouterState:this.routerState}),d.catch(Promise.reject.bind(Promise))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();function VO(t){for(let n=0;n<t.length;n++)if(t[n]==null)throw new w(4008,!1)}var HO=new b("");function Ug(t,...n){return Fn([{provide:vl,multi:!0,useValue:t},{provide:Qi,useFactory:UO},{provide:Ha,multi:!0,useFactory:zO},n.map(e=>e.\u0275providers)])}function UO(){return u(Lu).routerState.root}function zO(){let t=u(R);return n=>{let e=t.get(Ft);if(n!==e.components[0])return;let i=t.get(Lu),r=t.get($O);t.get(GO)===1&&i.initialNavigation(),t.get(WO,null,{optional:!0})?.setUpPreloading(),t.get(HO,null,{optional:!0})?.init(),i.resetRootComponentType(e.componentTypes[0]),r.closed||(r.next(),r.complete(),r.unsubscribe())}}var $O=new b("",{factory:()=>new x}),GO=new b("",{factory:()=>1});var WO=new b("");var Vu=new b("LABEL_RENDERER");var zg=7.9921259842519685,bl=[{key:"50x30",label:"50 x 30 mm",widthMm:50,heightMm:30},{key:"60x40",label:"60 x 40 mm",widthMm:60,heightMm:40},{key:"80x50",label:"80 x 50 mm",widthMm:80,heightMm:50},{key:"100x50",label:"100 x 50 mm",widthMm:100,heightMm:50},{key:"100x150",label:"100 x 150 mm",widthMm:100,heightMm:150},{key:"102x152",label:'102 x 152 mm (4x6")',widthMm:102,heightMm:152}],$g="100x150",YO=bl.find(t=>t.key===$g)??bl[0];function _l(t){return bl.find(n=>n.key===t)??YO}function vi(t){return Math.round(t.widthMm*zg)}function bi(t){return Math.round(t.heightMm*zg)}function JC(t){return{sizePreset:t,elements:[]}}function Bu(t,n){return N(v({},t),{elements:[...t.elements,n]})}function eD(t,n){return N(v({},t),{elements:t.elements.filter(e=>e.id!==n)})}function tD(t,n,e){return N(v({},t),{elements:t.elements.map(i=>i.id===n?v(v({},i),e):i)})}function nD(t,n,e){let i=Math.ceil(t/8),r=new Uint8Array(i*n);for(let o=0;o<n;o++){let s=o*i;for(let a=0;a<t;a++){if(!e(a,o))continue;let l=s+(a>>3),c=7-a%8;r[l]|=1<<c}}return{widthDots:t,heightDots:n,bytesPerRow:i,packedRows:r}}function Gg(){return`${Date.now().toString(36)}-${Math.random().toString(36).slice(2,10)}`}function iD(t){let n=t.widthDots??200,e=t.heightDots??80;return{id:Gg(),kind:"text",xDots:Math.round(t.centerXDots-n/2),yDots:Math.round(t.centerYDots-e/2),widthDots:n,heightDots:e,text:"Novo texto",fontSizeDots:28,bold:!1,align:"left",verticalAlign:"top",rotationDeg:0}}function rD(t,n){return N(v({},t),{id:Gg(),xDots:n.xDots,yDots:n.yDots})}function oD(t){let n=t.naturalWidth/t.naturalHeight,e=Math.min(t.naturalWidth,t.maxWidthDots),i=e/n;return i>t.maxHeightDots&&(i=t.maxHeightDots,e=i*n),e=Math.max(8,Math.round(e)),i=Math.max(8,Math.round(i)),{id:Gg(),kind:"image",xDots:Math.round(t.centerXDots-e/2),yDots:Math.round(t.centerYDots-i/2),widthDots:e,heightDots:i,imageDataUrl:t.imageDataUrl}}function ms(t){return t.split(`
`)}function so(t){return Math.round(t*1.2)}function ps(t,n,e){return e===90||e===270?{width:n,height:t}:{width:t,height:n}}function XO(t,n){return(t-(n.ascent+n.descent))/2}function sD(t,n){return XO(t,n)+n.ascent}function KO(t,n){return t.ascent+t.descent>0?t:N(v({},t),{ascent:n.ascent,descent:n.descent})}function aD(t,n,e,i){let r=sD(n,e);if(t.length===0)return{baselineOffset:r,inkTop:r,inkBottom:r,medidas:[]};let o=t.map(c=>KO(i(c),e)),s=o.length-1,a=r-o[0].ascent,l=s*n+r+o[s].descent;return{baselineOffset:r,inkTop:a,inkBottom:l,medidas:o}}function lD(t){let{lines:n,lineHeight:e,font:i,measureLine:r}=t,{inkTop:o,inkBottom:s,medidas:a}=aD(n,e,i,r);return{width:a.reduce((c,d)=>Math.max(c,d.left+d.right),0),height:s-o}}function ju(t){let{lines:n,lineHeight:e,boxWidth:i,boxHeight:r,align:o,verticalAlign:s,font:a,measureLine:l}=t;if(n.length===0)return{lines:[],baselineOffset:sD(e,a),inkHeight:0};let{baselineOffset:c,inkTop:d,inkBottom:f,medidas:h}=aD(n,e,a,l),p=f-d,S=s==="bottom"?r-f:s==="middle"?(r-p)/2-d:-d;return{lines:n.map(($,H)=>{let Se=h[H],Ge=o==="right"?i-Se.right:o==="center"?(i-(Se.right-Se.left))/2:Se.left;return{text:$,top:S+H*e,startX:Ge}}),baselineOffset:c,inkHeight:p}}function gs(t,n){return`${n?"bold ":""}${t}px Arial, sans-serif`}function vs(t){let n=t.measureText("M");return{ascent:n.fontBoundingBoxAscent,descent:n.fontBoundingBoxDescent}}function bs(t,n){let e=t.measureText(n);return{ascent:e.actualBoundingBoxAscent,descent:e.actualBoundingBoxDescent,left:e.actualBoundingBoxLeft,right:e.actualBoundingBoxRight}}var Wg=null;function Hu(){return Wg||(Wg=document.createElement("canvas").getContext("2d")),Wg}var cD=128,Uu=class t{async render(n){let e=vi(n.sizePreset),i=bi(n.sizePreset),r=document.createElement("canvas");r.width=e,r.height=i;let o=r.getContext("2d",{willReadFrequently:!0});if(!o)throw new Error("N\xE3o foi poss\xEDvel criar o contexto de desenho.");o.fillStyle="#ffffff",o.fillRect(0,0,e,i),o.fillStyle="#000000";for(let a of n.elements)a.kind==="text"?this.drawText(o,a):await this.drawImage(o,a);let s=o.getImageData(0,0,e,i);return nD(e,i,(a,l)=>{let c=(l*e+a)*4;if(s.data[c+3]<cD)return!1;let f=s.data[c],h=s.data[c+1],p=s.data[c+2];return .299*f+.587*h+.114*p<cD})}drawText(n,e){n.font=gs(e.fontSizeDots,e.bold),n.textAlign="left",n.textBaseline="alphabetic";let{width:i,height:r}=ps(e.widthDots,e.heightDots,e.rotationDeg),o=so(e.fontSizeDots),s=ju({lines:ms(e.text),lineHeight:o,boxWidth:i,boxHeight:r,align:e.align,verticalAlign:e.verticalAlign,font:vs(n),measureLine:f=>bs(n,f)}),a=-i/2,l=-r/2,c=e.xDots+e.widthDots/2,d=e.yDots+e.heightDots/2;n.save(),n.translate(c,d),e.rotationDeg!==0&&n.rotate(e.rotationDeg*Math.PI/180);for(let f of s.lines)n.fillText(f.text,a+f.startX,l+f.top+s.baselineOffset);n.restore()}async drawImage(n,e){let i=new Image;i.src=e.imageDataUrl,await i.decode(),n.drawImage(i,e.xDots,e.yDots,e.widthDots,e.heightDots)}static \u0275fac=function(e){return new(e||t)};static \u0275prov=G({token:t,factory:t.\u0275fac,providedIn:"root"})};var _s=new b("PRINTER_GATEWAY");var QO="127.0.0.1";function Ji(){return typeof window<"u"&&window.location.protocol==="https:"?"https":"http"}function ys(t=Ji()){return{host:QO,port:t==="https"?9101:9100}}function dD(t,n=Ji()){let e=ys(n);return t.host===e.host&&t.port===e.port}function uD(t){return`${t.host}:${t.port}`}function ao(t){try{let n=localStorage.getItem(t);return n?JSON.parse(n):null}catch{return null}}function Ss(t,n){try{return localStorage.setItem(t,JSON.stringify(n)),!0}catch{return!1}}var fD="zpl-label-studio.printer-connection.v2",JO="zpl-label-studio.printer-connection.v1";function qg(t){let n=ys(),e=typeof t?.host=="string"&&t.host.trim()?t.host.trim():n.host,i=t?.port,r=typeof i=="number"&&Number.isInteger(i)&&i>0&&i<=65535?i:n.port;return{host:e,port:r}}function eP(){let t=ao(fD);if(t?.host)return qg(t);let n=ao(JO);return qg(n?{host:n.host,port:n.port??void 0}:null)}var er=class t{configSignal=U(eP());config=this.configSignal.asReadonly();isCustom=K(()=>!dD(this.configSignal()));constructor(){this.gravar(this.configSignal())}update(n){this.gravar(qg(n))}reset(){this.gravar(ys())}gravar(n){this.configSignal.set(n),Ss(fD,n)}static \u0275fac=function(e){return new(e||t)};static \u0275prov=G({token:t,factory:t.\u0275fac,providedIn:"root"})};var yl=class extends Error{},Yg=class extends Error{},tr=class t{settings=u(er);get baseUrl(){let{host:n,port:e}=this.settings.config();return`${Ji()}://${n}:${e}`}get sslSupportUrl(){return`${this.baseUrl}/ssl_support`}async getDefaultPrinter(){let n=await this.fetchFromAgent("/default?type=printer");if(!n.ok)return null;let e=await n.json();return this.toPrinterDevice(e)}async listAvailablePrinters(){let n=await this.fetchFromAgent("/available");return n.ok?((await n.json()).printer??[]).map(i=>this.toPrinterDevice(i)).filter(i=>i!==null):[]}async write(n,e){let i=await this.fetchFromAgent("/write",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({device:n,data:e})});if(!i.ok)throw new Yg(`O agente Zebra Browser Print recusou a impress\xE3o (HTTP ${i.status}).`)}async fetchFromAgent(n,e){try{return await fetch(`${this.baseUrl}${n}`,e)}catch(i){throw new yl("N\xE3o foi poss\xEDvel falar com o Zebra Browser Print. Ele precisa estar instalado e em execu\xE7\xE3o.",{cause:i})}}toPrinterDevice(n){return!n||!("name"in n)||!n.name?null:{name:n.name,uid:n.uid,connection:n.connection,deviceType:n.deviceType,version:n.version,provider:n.provider,manufacturer:n.manufacturer}}static \u0275fac=function(e){return new(e||t)};static \u0275prov=G({token:t,factory:t.\u0275fac,providedIn:"root"})};var zu=class t{constructor(n){this.client=n}client;detectDefaultPrinter(){return this.client.getDefaultPrinter()}sendRaw(n,e){return this.client.write(n,e)}static \u0275fac=function(e){return new(e||t)(A(tr))};static \u0275prov=G({token:t,factory:t.\u0275fac,providedIn:"root"})};var Sl=class{_attachedHost=null;attach(n){return this._attachedHost=n,n.attach(this)}detach(){let n=this._attachedHost;n!=null&&(this._attachedHost=null,n.detach())}get isAttached(){return this._attachedHost!=null}setAttachedHost(n){this._attachedHost=n}},En=class extends Sl{component;viewContainerRef;injector;projectableNodes;bindings;directives;constructor(n,e,i,r,o,s){super(),this.component=n,this.viewContainerRef=e,this.injector=i,this.projectableNodes=r,this.bindings=o||null,this.directives=s||null}},xn=class extends Sl{templateRef;viewContainerRef;context;injector;constructor(n,e,i,r){super(),this.templateRef=n,this.viewContainerRef=e,this.context=i,this.injector=r}get origin(){return this.templateRef.elementRef}attach(n,e=this.context){return this.context=e,super.attach(n)}detach(){return this.context=void 0,super.detach()}},Zg=class extends Sl{element;constructor(n){super(),this.element=n instanceof O?n.nativeElement:n}},nr=class{_attachedPortal=null;_disposeFn=null;_isDisposed=!1;hasAttached(){return!!this._attachedPortal}attach(n){if(n instanceof En)return this._attachedPortal=n,this.attachComponentPortal(n);if(n instanceof xn)return this._attachedPortal=n,this.attachTemplatePortal(n);if(this.attachDomPortal&&n instanceof Zg)return this._attachedPortal=n,this.attachDomPortal(n)}attachDomPortal=null;detach(){this._attachedPortal&&(this._attachedPortal.setAttachedHost(null),this._attachedPortal=null),this._invokeDisposeFn()}dispose(){this.hasAttached()&&this.detach(),this._invokeDisposeFn(),this._isDisposed=!0}setDisposeFn(n){this._disposeFn=n}_invokeDisposeFn(){this._disposeFn&&(this._disposeFn(),this._disposeFn=null)}},Cl=class extends nr{outletElement;_appRef;_defaultInjector;constructor(n,e,i){super(),this.outletElement=n,this._appRef=e,this._defaultInjector=i}attachComponentPortal(n){let e;if(n.viewContainerRef){let i=n.injector||n.viewContainerRef.injector,r=i.get(zn,null,{optional:!0})||void 0;e=n.viewContainerRef.createComponent(n.component,{index:n.viewContainerRef.length,injector:i,ngModuleRef:r,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0,directives:n.directives||void 0}),this.setDisposeFn(()=>e.destroy())}else{let i=this._appRef,r=n.injector||this._defaultInjector||R.NULL,o=r.get(Me,i.injector);e=iu(n.component,{elementInjector:r,environmentInjector:o,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0,directives:n.directives||void 0}),i.attachView(e.hostView),this.setDisposeFn(()=>{i.viewCount>0&&i.detachView(e.hostView),e.destroy()})}return this.outletElement.appendChild(this._getComponentRootNode(e)),this._attachedPortal=n,e}attachTemplatePortal(n){let e=n.viewContainerRef,i=e.createEmbeddedView(n.templateRef,n.context,{injector:n.injector});return i.rootNodes.forEach(r=>this.outletElement.appendChild(r)),i.detectChanges(),this.setDisposeFn(()=>{let r=e.indexOf(i);r!==-1&&e.remove(r)}),this._attachedPortal=n,i}attachDomPortal=n=>{let e=n.element;e.parentNode;let i=this.outletElement.ownerDocument.createComment("dom-portal");e.parentNode.insertBefore(i,e),this.outletElement.appendChild(e),this._attachedPortal=n,super.setDisposeFn(()=>{i.parentNode&&i.parentNode.replaceChild(e,i)})};dispose(){super.dispose(),this.outletElement.remove()}_getComponentRootNode(n){return n.hostView.rootNodes[0]}};var qn=(()=>{class t extends nr{_moduleRef=u(zn,{optional:!0});_document=u(j);_viewContainerRef=u(bt);_isInitialized=!1;_attachedRef=null;get portal(){return this._attachedPortal}set portal(e){this.hasAttached()&&!e&&!this._isInitialized||(this.hasAttached()&&super.detach(),e&&super.attach(e),this._attachedPortal=e||null)}attached=new W;get attachedRef(){return this._attachedRef}ngOnInit(){this._isInitialized=!0}ngOnDestroy(){super.dispose(),this._attachedRef=this._attachedPortal=null}attachComponentPortal(e){e.setAttachedHost(this);let i=e.viewContainerRef!=null?e.viewContainerRef:this._viewContainerRef,r=i.createComponent(e.component,{index:i.length,injector:e.injector||i.injector,projectableNodes:e.projectableNodes||void 0,ngModuleRef:this._moduleRef||void 0,bindings:e.bindings||void 0,directives:e.directives||void 0});return i!==this._viewContainerRef&&this._getRootNode().appendChild(r.hostView.rootNodes[0]),super.setDisposeFn(()=>r.destroy()),this._attachedPortal=e,this._attachedRef=r,this.attached.emit(r),r}attachTemplatePortal(e){e.setAttachedHost(this);let i=this._viewContainerRef.createEmbeddedView(e.templateRef,e.context,{injector:e.injector});return super.setDisposeFn(()=>this._viewContainerRef.clear()),this._attachedPortal=e,this._attachedRef=i,this.attached.emit(i),i}attachDomPortal=e=>{let i=e.element;i.parentNode;let r=this._document.createComment("dom-portal");e.setAttachedHost(this),i.parentNode.insertBefore(r,i),this._getRootNode().appendChild(i),this._attachedPortal=e,super.setDisposeFn(()=>{r.parentNode&&r.parentNode.replaceChild(i,r)})};_getRootNode(){let e=this._viewContainerRef.element.nativeElement;return e.nodeType===e.ELEMENT_NODE?e:e.parentNode}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ht(t)))(r||t)}})();static \u0275dir=z({type:t,selectors:[["","cdkPortalOutlet",""]],inputs:{portal:[0,"cdkPortalOutlet","portal"]},outputs:{attached:"attached"},exportAs:["cdkPortalOutlet"],features:[Te]})}return t})(),_i=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({})}return t})();function lo(t){return t.buttons===0||t.detail===0}function co(t){let n=t.touches&&t.touches[0]||t.changedTouches&&t.changedTouches[0];return!!n&&n.identifier===-1&&(n.radiusX==null||n.radiusX===1)&&(n.radiusY==null||n.radiusY===1)}var Xg;function hD(){if(Xg==null){let t=typeof document<"u"?document.head:null;Xg=!!(t&&(t.createShadowRoot||t.attachShadow))}return Xg}function Kg(t){if(hD()){let n=t.getRootNode?t.getRootNode():null;if(typeof ShadowRoot<"u"&&ShadowRoot&&n instanceof ShadowRoot)return n}return null}function Dl(){let t=typeof document<"u"&&document?document.activeElement:null;for(;t&&t.shadowRoot;){let n=t.shadowRoot.activeElement;if(n===t)break;t=n}return t}function yt(t){if(t.composedPath)try{return t.composedPath()[0]}catch{}return t.target}var Qg;try{Qg=typeof Intl<"u"&&Intl.v8BreakIterator}catch{Qg=!1}var be=(()=>{class t{_platformId=u(zr);isBrowser=this._platformId?PS(this._platformId):typeof document=="object"&&!!document;EDGE=this.isBrowser&&/(edge)/i.test(navigator.userAgent);TRIDENT=this.isBrowser&&/(msie|trident)/i.test(navigator.userAgent);BLINK=this.isBrowser&&!!(window.chrome||Qg)&&typeof CSS<"u"&&!this.EDGE&&!this.TRIDENT;WEBKIT=this.isBrowser&&/AppleWebKit/i.test(navigator.userAgent)&&!this.BLINK&&!this.EDGE&&!this.TRIDENT;IOS=this.isBrowser&&/iPad|iPhone|iPod/.test(navigator.userAgent)&&!("MSStream"in window);FIREFOX=this.isBrowser&&/(firefox|minefield)/i.test(navigator.userAgent);ANDROID=this.isBrowser&&/android/i.test(navigator.userAgent)&&!this.TRIDENT;SAFARI=this.isBrowser&&/safari/i.test(navigator.userAgent)&&this.WEBKIT;static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();var El;function mD(){if(El==null&&typeof window<"u")try{window.addEventListener("test",null,Object.defineProperty({},"passive",{get:()=>El=!0}))}finally{El=El||!1}return El}function Cs(t){return mD()?t:!!t.capture}function yi(t,n=0){return pD(t)?Number(t):arguments.length===2?n:0}function pD(t){return!isNaN(parseFloat(t))&&!isNaN(Number(t))}function Wt(t){return t instanceof O?t.nativeElement:t}var gD=new b("cdk-input-modality-detector-options"),vD={ignoreKeys:[18,17,224,91,16]},bD=650,Jg={passive:!0,capture:!0},_D=(()=>{class t{_platform=u(be);_listenerCleanups;modalityDetected;modalityChanged;get mostRecentModality(){return this._modality.value}_mostRecentTarget=null;_modality=new tt(null);_options;_lastTouchMs=0;_onKeydown=e=>{this._options?.ignoreKeys?.some(i=>i===e.keyCode)||(this._modality.next("keyboard"),this._mostRecentTarget=yt(e))};_onMousedown=e=>{Date.now()-this._lastTouchMs<bD||(this._modality.next(lo(e)?"keyboard":"mouse"),this._mostRecentTarget=yt(e))};_onTouchstart=e=>{if(co(e)){this._modality.next("keyboard");return}this._lastTouchMs=Date.now(),this._modality.next("touch"),this._mostRecentTarget=yt(e)};constructor(){let e=u(k),i=u(j),r=u(gD,{optional:!0});if(this._options=v(v({},vD),r),this.modalityDetected=this._modality.pipe(aa(1)),this.modalityChanged=this.modalityDetected.pipe(Pc()),this._platform.isBrowser){let o=u(Qe).createRenderer(null,null);this._listenerCleanups=e.runOutsideAngular(()=>[o.listen(i,"keydown",this._onKeydown,Jg),o.listen(i,"mousedown",this._onMousedown,Jg),o.listen(i,"touchstart",this._onTouchstart,Jg)])}}ngOnDestroy(){this._modality.complete(),this._listenerCleanups?.forEach(e=>e())}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})(),xl=(function(t){return t[t.IMMEDIATE=0]="IMMEDIATE",t[t.EVENTUAL=1]="EVENTUAL",t})(xl||{}),yD=new b("cdk-focus-monitor-default-options"),$u=Cs({passive:!0,capture:!0}),sn=(()=>{class t{_ngZone=u(k);_platform=u(be);_inputModalityDetector=u(_D);_origin=null;_lastFocusOrigin=null;_windowFocused=!1;_windowFocusTimeoutId;_originTimeoutId;_originFromTouchInteraction=!1;_elementInfo=new Map;_monitoredElementCount=0;_rootNodeFocusListenerCount=new Map;_detectionMode;_windowFocusListener=()=>{this._windowFocused=!0,this._windowFocusTimeoutId=setTimeout(()=>this._windowFocused=!1)};_document=u(j);_stopInputModalityDetector=new x;constructor(){let e=u(yD,{optional:!0});this._detectionMode=e?.detectionMode||xl.IMMEDIATE}_rootNodeFocusAndBlurListener=e=>{let i=yt(e);for(let r=i;r;r=r.parentElement)e.type==="focus"?this._onFocus(e,r):this._onBlur(e,r)};monitor(e,i=!1){let r=Wt(e);if(!this._platform.isBrowser||r.nodeType!==1)return Y();let o=Kg(r)||this._document,s=this._elementInfo.get(r);if(s)return i&&(s.checkChildren=!0),s.subject;let a={checkChildren:i,subject:new x,rootNode:o};return this._elementInfo.set(r,a),this._registerGlobalListeners(a),a.subject}stopMonitoring(e){let i=Wt(e),r=this._elementInfo.get(i);r&&(r.subject.complete(),this._setClasses(i),this._elementInfo.delete(i),this._removeGlobalListeners(r))}focusVia(e,i,r){let o=Wt(e),s=this._document.activeElement;o===s?this._getClosestElementsInfo(o).forEach(([a,l])=>this._originChanged(a,i,l)):(this._setOrigin(i),typeof o.focus=="function"&&o.focus(r))}ngOnDestroy(){this._elementInfo.forEach((e,i)=>this.stopMonitoring(i))}_getWindow(){return this._document.defaultView||window}_getFocusOrigin(e){return this._origin?this._originFromTouchInteraction?this._shouldBeAttributedToTouch(e)?"touch":"program":this._origin:this._windowFocused&&this._lastFocusOrigin?this._lastFocusOrigin:e&&this._isLastInteractionFromInputLabel(e)?"mouse":"program"}_shouldBeAttributedToTouch(e){return this._detectionMode===xl.EVENTUAL||!!e?.contains(this._inputModalityDetector._mostRecentTarget)}_setClasses(e,i){e.classList.toggle("cdk-focused",!!i),e.classList.toggle("cdk-touch-focused",i==="touch"),e.classList.toggle("cdk-keyboard-focused",i==="keyboard"),e.classList.toggle("cdk-mouse-focused",i==="mouse"),e.classList.toggle("cdk-program-focused",i==="program")}_setOrigin(e,i=!1){this._ngZone.runOutsideAngular(()=>{if(this._origin=e,this._originFromTouchInteraction=e==="touch"&&i,this._detectionMode===xl.IMMEDIATE){clearTimeout(this._originTimeoutId);let r=this._originFromTouchInteraction?bD:1;this._originTimeoutId=setTimeout(()=>this._origin=null,r)}})}_onFocus(e,i){let r=this._elementInfo.get(i),o=yt(e);!r||!r.checkChildren&&i!==o||this._originChanged(i,this._getFocusOrigin(o),r)}_onBlur(e,i){let r=this._elementInfo.get(i);!r||r.checkChildren&&e.relatedTarget instanceof Node&&i.contains(e.relatedTarget)||(this._setClasses(i),this._emitOrigin(r,null))}_emitOrigin(e,i){e.subject.observers.length&&this._ngZone.run(()=>e.subject.next(i))}_registerGlobalListeners(e){if(!this._platform.isBrowser)return;let i=e.rootNode,r=this._rootNodeFocusListenerCount.get(i)||0;r||this._ngZone.runOutsideAngular(()=>{i.addEventListener("focus",this._rootNodeFocusAndBlurListener,$u),i.addEventListener("blur",this._rootNodeFocusAndBlurListener,$u)}),this._rootNodeFocusListenerCount.set(i,r+1),++this._monitoredElementCount===1&&(this._ngZone.runOutsideAngular(()=>{this._getWindow().addEventListener("focus",this._windowFocusListener)}),this._inputModalityDetector.modalityDetected.pipe(Ce(this._stopInputModalityDetector)).subscribe(o=>{this._setOrigin(o,!0)}))}_removeGlobalListeners(e){let i=e.rootNode;if(this._rootNodeFocusListenerCount.has(i)){let r=this._rootNodeFocusListenerCount.get(i);r>1?this._rootNodeFocusListenerCount.set(i,r-1):(i.removeEventListener("focus",this._rootNodeFocusAndBlurListener,$u),i.removeEventListener("blur",this._rootNodeFocusAndBlurListener,$u),this._rootNodeFocusListenerCount.delete(i))}--this._monitoredElementCount||(this._getWindow().removeEventListener("focus",this._windowFocusListener),this._stopInputModalityDetector.next(),clearTimeout(this._windowFocusTimeoutId),clearTimeout(this._originTimeoutId))}_originChanged(e,i,r){this._setClasses(e,i),this._emitOrigin(r,i),this._lastFocusOrigin=i}_getClosestElementsInfo(e){let i=[];return this._elementInfo.forEach((r,o)=>{(o===e||r.checkChildren&&o.contains(e))&&i.push([o,r])}),i}_isLastInteractionFromInputLabel(e){let{_mostRecentTarget:i,mostRecentModality:r}=this._inputModalityDetector;if(r!=="mouse"||!i||i===e||e.nodeName!=="INPUT"&&e.nodeName!=="TEXTAREA"||e.disabled)return!1;let o=e.labels;if(o){for(let s=0;s<o.length;s++)if(o[s].contains(i))return!0}return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();var Gu=new WeakMap,ze=(()=>{class t{_appRef;_injector=u(R);_environmentInjector=u(Me);load(e){let i=this._appRef=this._appRef||this._injector.get(Ft),r=Gu.get(i);r||(r={loaders:new Set,refs:[]},Gu.set(i,r),i.onDestroy(()=>{Gu.get(i)?.refs.forEach(o=>o.destroy()),Gu.delete(i)})),r.loaders.has(e)||(r.loaders.add(e),r.refs.push(iu(e,{environmentInjector:this._environmentInjector})))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();var Ds=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["ng-component"]],exportAs:["cdkVisuallyHidden"],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-visually-hidden {
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
`],encapsulation:2})}return t})(),Wu;function nP(){if(Wu===void 0&&(Wu=null,typeof window<"u")){let t=window;if(t.trustedTypes!==void 0)try{Wu=t.trustedTypes.createPolicy("angular#components",{createHTML:n=>n})}catch(n){console.error(n)}}return Wu}function uo(t){return nP()?.createHTML(t)||t}function SD(t,n,e){let i=e.sanitize(st.HTML,n);t.innerHTML=uo(i||"")}function Es(t){return Array.isArray(t)?t:[t]}var CD=new Set,fo,xs=(()=>{class t{_platform=u(be);_nonce=u(Bi,{optional:!0});_matchMedia;constructor(){this._matchMedia=this._platform.isBrowser&&window.matchMedia?window.matchMedia.bind(window):rP}matchMedia(e){return(this._platform.WEBKIT||this._platform.BLINK)&&iP(e,this._nonce),this._matchMedia(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();function iP(t,n){if(!CD.has(t))try{fo||(fo=document.createElement("style"),n&&fo.setAttribute("nonce",n),fo.setAttribute("type","text/css"),document.head.appendChild(fo)),fo.sheet&&(fo.sheet.insertRule(`@media ${t.replace(/[{}]/g,"")} {body{ }}`,0),CD.add(t))}catch(e){console.error(e)}}function rP(t){return{matches:t==="all"||t==="",media:t,addListener:()=>{},removeListener:()=>{}}}var ho=(()=>{class t{_mediaMatcher=u(xs);_zone=u(k);_queries=new Map;_destroySubject=new x;ngOnDestroy(){this._destroySubject.next(),this._destroySubject.complete()}isMatched(e){return DD(Es(e)).some(r=>this._registerQuery(r).mql.matches)}observe(e){let r=DD(Es(e)).map(s=>this._registerQuery(s).observable),o=ia(r);return o=Ai(o.pipe(Ae(1)),o.pipe(aa(1),oa(0))),o.pipe(oe(s=>{let a={matches:!1,breakpoints:{}};return s.forEach(({matches:l,query:c})=>{a.matches=a.matches||l,a.breakpoints[c]=l}),a}))}_registerQuery(e){if(this._queries.has(e))return this._queries.get(e);let i=this._mediaMatcher.matchMedia(e),o={observable:new ne(s=>{let a=l=>this._zone.run(()=>s.next(l));return i.addListener(a),()=>{i.removeListener(a)}}).pipe(nt(i),oe(({matches:s})=>({query:e,matches:s})),Ce(this._destroySubject)),mql:i};return this._queries.set(e,o),o}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();function DD(t){return t.map(n=>n.split(",")).reduce((n,e)=>n.concat(e)).map(n=>n.trim())}var oP=(()=>{class t{create(e){return typeof MutationObserver>"u"?null:new MutationObserver(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();var qu=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({providers:[oP]})}return t})();var nv=(()=>{class t{_platform=u(be);isDisabled(e){return e.hasAttribute("disabled")}isVisible(e){return aP(e)&&getComputedStyle(e).visibility==="visible"}isTabbable(e){if(!this._platform.isBrowser)return!1;let i=sP(pP(e));if(i&&(ED(i)===-1||!this.isVisible(i)))return!1;let r=e.nodeName.toLowerCase(),o=ED(e);return e.hasAttribute("contenteditable")?o!==-1:r==="iframe"||r==="object"||this._platform.WEBKIT&&this._platform.IOS&&!hP(e)?!1:r==="audio"?e.hasAttribute("controls")?o!==-1:!1:r==="video"?o===-1?!1:o!==null?!0:this._platform.FIREFOX||e.hasAttribute("controls"):e.tabIndex>=0}isFocusable(e,i){return mP(e)&&!this.isDisabled(e)&&(i?.ignoreVisibility||this.isVisible(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();function sP(t){try{return t.frameElement}catch{return null}}function aP(t){return!!(t.offsetWidth||t.offsetHeight||typeof t.getClientRects=="function"&&t.getClientRects().length)}function lP(t){let n=t.nodeName.toLowerCase();return n==="input"||n==="select"||n==="button"||n==="textarea"}function cP(t){return uP(t)&&t.type=="hidden"}function dP(t){return fP(t)&&t.hasAttribute("href")}function uP(t){return t.nodeName.toLowerCase()=="input"}function fP(t){return t.nodeName.toLowerCase()=="a"}function ID(t){if(!t.hasAttribute("tabindex")||t.tabIndex===void 0)return!1;let n=t.getAttribute("tabindex");return!!(n&&!isNaN(parseInt(n,10)))}function ED(t){if(!ID(t))return null;let n=parseInt(t.getAttribute("tabindex")||"",10);return isNaN(n)?-1:n}function hP(t){let n=t.nodeName.toLowerCase(),e=n==="input"&&t.type;return e==="text"||e==="password"||n==="select"||n==="textarea"}function mP(t){return cP(t)?!1:lP(t)||dP(t)||t.hasAttribute("contenteditable")||ID(t)}function pP(t){return t.ownerDocument&&t.ownerDocument.defaultView||window}var tv=class{_element;_checker;_ngZone;_document;_injector;_startAnchor=null;_endAnchor=null;_hasAttached=!1;startAnchorListener=()=>{!this.focusLastTabbableElement()&&this._checker.isFocusable(this._element)&&this._element.focus()};endAnchorListener=()=>{!this.focusFirstTabbableElement()&&this._checker.isFocusable(this._element)&&this._element.focus()};get enabled(){return this._enabled}set enabled(n){this._enabled=n,this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(n,this._startAnchor),this._toggleAnchorTabIndex(n,this._endAnchor))}_enabled=!0;constructor(n,e,i,r,o=!1,s){this._element=n,this._checker=e,this._ngZone=i,this._document=r,this._injector=s,o||this.attachAnchors()}destroy(){let n=this._startAnchor,e=this._endAnchor;n&&(n.removeEventListener("focus",this.startAnchorListener),n.remove()),e&&(e.removeEventListener("focus",this.endAnchorListener),e.remove()),this._startAnchor=this._endAnchor=null,this._hasAttached=!1}attachAnchors(){return this._hasAttached?!0:(this._ngZone.runOutsideAngular(()=>{this._startAnchor||(this._startAnchor=this._createAnchor(),this._startAnchor.addEventListener("focus",this.startAnchorListener)),this._endAnchor||(this._endAnchor=this._createAnchor(),this._endAnchor.addEventListener("focus",this.endAnchorListener))}),this._element.parentNode&&(this._element.parentNode.insertBefore(this._startAnchor,this._element),this._element.parentNode.insertBefore(this._endAnchor,this._element.nextSibling),this._hasAttached=!0),this._hasAttached)}focusInitialElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusInitialElement(n)))})}focusFirstTabbableElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusFirstTabbableElement(n)))})}focusLastTabbableElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusLastTabbableElement(n)))})}_getRegionBoundary(n){let e=this._element.querySelectorAll(`[cdk-focus-region-${n}], [cdkFocusRegion${n}], [cdk-focus-${n}]`);return n=="start"?e.length?e[0]:this._getFirstTabbableElement(this._element):e.length?e[e.length-1]:this._getLastTabbableElement(this._element)}focusInitialElement(n){let e=this._element.querySelector("[cdk-focus-initial], [cdkFocusInitial]");if(e){if(!this._checker.isFocusable(e)){let i=this._getFirstTabbableElement(e);return i?.focus(n),!!i}return e.focus(n),!0}return this.focusFirstTabbableElement(n)}focusFirstTabbableElement(n){let e=this._getRegionBoundary("start");return e&&e.focus(n),!!e}focusLastTabbableElement(n){let e=this._getRegionBoundary("end");return e&&e.focus(n),!!e}hasAttached(){return this._hasAttached}_getFirstTabbableElement(n){if(this._checker.isFocusable(n)&&this._checker.isTabbable(n))return n;let e=n.children;for(let i=0;i<e.length;i++){let r=e[i].nodeType===this._document.ELEMENT_NODE?this._getFirstTabbableElement(e[i]):null;if(r)return r}return null}_getLastTabbableElement(n){if(this._checker.isFocusable(n)&&this._checker.isTabbable(n))return n;let e=n.children;for(let i=e.length-1;i>=0;i--){let r=e[i].nodeType===this._document.ELEMENT_NODE?this._getLastTabbableElement(e[i]):null;if(r)return r}return null}_createAnchor(){let n=this._document.createElement("div");return this._toggleAnchorTabIndex(this._enabled,n),n.classList.add("cdk-visually-hidden"),n.classList.add("cdk-focus-trap-anchor"),n.setAttribute("aria-hidden","true"),n}_toggleAnchorTabIndex(n,e){n?e.setAttribute("tabindex","0"):e.removeAttribute("tabindex")}toggleAnchors(n){this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(n,this._startAnchor),this._toggleAnchorTabIndex(n,this._endAnchor))}_executeOnStable(n){Je(n,{injector:this._injector})}},iv=(()=>{class t{_checker=u(nv);_ngZone=u(k);_document=u(j);_injector=u(R);constructor(){u(ze).load(Ds)}create(e,i=!1){return new tv(e,this._checker,this._ngZone,this._document,i,this._injector)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();var MD=new b("liveAnnouncerElement",{providedIn:"root",factory:()=>null}),TD=new b("LIVE_ANNOUNCER_DEFAULT_OPTIONS"),gP=0,wl=(()=>{class t{_ngZone=u(k);_defaultOptions=u(TD,{optional:!0});_liveElement;_document=u(j);_sanitizer=u(Qa);_previousTimeout;_currentPromise;_currentResolve;constructor(){let e=u(MD,{optional:!0});this._liveElement=e||this._createLiveElement()}announce(e,...i){let r=this._defaultOptions,o,s;return i.length===1&&typeof i[0]=="number"?s=i[0]:[o,s]=i,this.clear(),clearTimeout(this._previousTimeout),o||(o=r&&r.politeness?r.politeness:"polite"),s==null&&r&&(s=r.duration),this._liveElement.setAttribute("aria-live",o),this._liveElement.id&&this._exposeAnnouncerToModals(this._liveElement.id),this._ngZone.runOutsideAngular(()=>(this._currentPromise||(this._currentPromise=new Promise(a=>this._currentResolve=a)),clearTimeout(this._previousTimeout),this._previousTimeout=setTimeout(()=>{!e||typeof e=="string"?this._liveElement.textContent=e:SD(this._liveElement,e,this._sanitizer),typeof s=="number"&&(this._previousTimeout=setTimeout(()=>this.clear(),s)),this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0},100),this._currentPromise))}clear(){this._liveElement&&(this._liveElement.textContent="")}ngOnDestroy(){clearTimeout(this._previousTimeout),this._liveElement?.remove(),this._liveElement=null,this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0}_createLiveElement(){let e="cdk-live-announcer-element",i=this._document.getElementsByClassName(e),r=this._document.createElement("div");for(let o=0;o<i.length;o++)i[o].remove();return r.classList.add(e),r.classList.add("cdk-visually-hidden"),r.setAttribute("aria-atomic","true"),r.setAttribute("aria-live","polite"),r.id=`cdk-live-announcer-${gP++}`,this._document.body.appendChild(r),r}_exposeAnnouncerToModals(e){let i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],s=o.getAttribute("aria-owns");s?s.indexOf(e)===-1&&o.setAttribute("aria-owns",s+" "+e):o.setAttribute("aria-owns",e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();var ir=(function(t){return t[t.NONE=0]="NONE",t[t.BLACK_ON_WHITE=1]="BLACK_ON_WHITE",t[t.WHITE_ON_BLACK=2]="WHITE_ON_BLACK",t})(ir||{}),xD="cdk-high-contrast-black-on-white",wD="cdk-high-contrast-white-on-black",ev="cdk-high-contrast-active",ND=(()=>{class t{_platform=u(be);_hasCheckedHighContrastMode=!1;_document=u(j);_breakpointSubscription;constructor(){this._breakpointSubscription=u(ho).observe("(forced-colors: active)").subscribe(()=>{this._hasCheckedHighContrastMode&&(this._hasCheckedHighContrastMode=!1,this._applyBodyHighContrastModeCssClasses())})}getHighContrastMode(){if(!this._platform.isBrowser)return ir.NONE;let e=this._document.createElement("div");e.style.backgroundColor="rgb(1,2,3)",e.style.position="absolute",this._document.body.appendChild(e);let i=this._document.defaultView||window,r=i&&i.getComputedStyle?i.getComputedStyle(e):null,o=(r&&r.backgroundColor||"").replace(/ /g,"");switch(e.remove(),o){case"rgb(0,0,0)":case"rgb(45,50,54)":case"rgb(32,32,32)":return ir.WHITE_ON_BLACK;case"rgb(255,255,255)":case"rgb(255,250,239)":return ir.BLACK_ON_WHITE}return ir.NONE}ngOnDestroy(){this._breakpointSubscription.unsubscribe()}_applyBodyHighContrastModeCssClasses(){if(!this._hasCheckedHighContrastMode&&this._platform.isBrowser&&this._document.body){let e=this._document.body.classList;e.remove(ev,xD,wD),this._hasCheckedHighContrastMode=!0;let i=this.getHighContrastMode();i===ir.BLACK_ON_WHITE?e.add(ev,xD):i===ir.WHITE_ON_BLACK&&e.add(ev,wD)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})(),Il=(()=>{class t{constructor(){u(ND)._applyBodyHighContrastModeCssClasses()}static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({imports:[qu]})}return t})();function rv(){return typeof __karma__<"u"&&!!__karma__||typeof jasmine<"u"&&!!jasmine||typeof jest<"u"&&!!jest||typeof Mocha<"u"&&!!Mocha}function Xe(t){return t==null?"":typeof t=="string"?t:`${t}px`}var vP=new b("cdk-dir-doc",{providedIn:"root",factory:()=>u(j)}),bP=/^(ar|ckb|dv|he|iw|fa|nqo|ps|sd|ug|ur|yi|.*[-_](Adlm|Arab|Hebr|Nkoo|Rohg|Thaa))(?!.*[-_](Latn|Cyrl)($|-|_))($|-|_)/i;function kD(t){let n=t?.toLowerCase()||"";return n==="auto"&&typeof navigator<"u"&&navigator?.language?bP.test(navigator.language)?"rtl":"ltr":n==="rtl"?"rtl":"ltr"}var ct=(()=>{class t{get value(){return this.valueSignal()}valueSignal=U("ltr");change=new W;constructor(){let e=u(vP,{optional:!0});if(e){let i=e.body?e.body.dir:null,r=e.documentElement?e.documentElement.dir:null;this.valueSignal.set(kD(i||r||"ltr"))}}ngOnDestroy(){this.change.complete()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();var wn=(function(t){return t[t.NORMAL=0]="NORMAL",t[t.NEGATED=1]="NEGATED",t[t.INVERTED=2]="INVERTED",t})(wn||{}),Yu,mo;function Zu(){if(mo==null){if(typeof document!="object"||!document||typeof Element!="function"||!Element)return mo=!1,mo;if(document.documentElement?.style&&"scrollBehavior"in document.documentElement.style)mo=!0;else{let t=Element.prototype.scrollTo;t?mo=!/\{\s*\[native code\]\s*\}/.test(t.toString()):mo=!1}}return mo}function ws(){if(typeof document!="object"||!document)return wn.NORMAL;if(Yu==null){let t=document.createElement("div"),n=t.style;t.dir="rtl",n.width="1px",n.overflow="auto",n.visibility="hidden",n.pointerEvents="none",n.position="absolute";let e=document.createElement("div"),i=e.style;i.width="2px",i.height="1px",t.appendChild(e),document.body.appendChild(t),Yu=wn.NORMAL,t.scrollLeft===0&&(t.scrollLeft=1,Yu=t.scrollLeft===0?wn.NEGATED:wn.INVERTED),t.remove()}return Yu}var ve=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({})}return t})();var _P=20,rr=(()=>{class t{_ngZone=u(k);_platform=u(be);_renderer=u(Qe).createRenderer(null,null);_cleanupGlobalListener;_scrolled=new x;_scrolledCount=0;scrollContainers=new Map;register(e){this.scrollContainers.has(e)||this.scrollContainers.set(e,e.elementScrolled().subscribe(()=>this._scrolled.next(e)))}deregister(e){let i=this.scrollContainers.get(e);i&&(i.unsubscribe(),this.scrollContainers.delete(e))}scrolled(e=_P){return this._platform.isBrowser?new ne(i=>{this._cleanupGlobalListener||(this._cleanupGlobalListener=this._ngZone.runOutsideAngular(()=>this._renderer.listen("document","scroll",()=>this._scrolled.next())));let r=e>0?this._scrolled.pipe(Oc(e)).subscribe(i):this._scrolled.subscribe(i);return this._scrolledCount++,()=>{r.unsubscribe(),this._scrolledCount--,this._scrolledCount||(this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0)}}):Y()}ngOnDestroy(){this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0,this.scrollContainers.forEach((e,i)=>this.deregister(i)),this._scrolled.complete()}ancestorScrolled(e,i){let r=this.getAncestorScrollContainers(e);return this.scrolled(i).pipe(ge(o=>!o||r.indexOf(o)>-1))}getAncestorScrollContainers(e){let i=[];return this.scrollContainers.forEach((r,o)=>{this._targetContainsElement(o,e)&&i.push(o)}),i}_targetContainsElement(e,i){let r=Wt(i),o=e.getElementRef().nativeElement;do if(r==o)return!0;while(r=r.parentElement);return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})(),ov=(()=>{class t{elementRef=u(O);scrollDispatcher=u(rr);ngZone=u(k);dir=u(ct,{optional:!0});_scrollElement=this.elementRef.nativeElement;_destroyed=new x;_renderer=u(Re);_cleanupScroll;_elementScrolled=new x;ngOnInit(){this._cleanupScroll=this.ngZone.runOutsideAngular(()=>this._renderer.listen(this._scrollElement,"scroll",e=>this._elementScrolled.next(e))),this.scrollDispatcher.register(this)}ngOnDestroy(){this._cleanupScroll?.(),this._elementScrolled.complete(),this.scrollDispatcher.deregister(this),this._destroyed.next(),this._destroyed.complete()}elementScrolled(){return this._elementScrolled}getElementRef(){return this.elementRef}scrollTo(e){let i=this.elementRef.nativeElement,r=this.dir&&this.dir.value=="rtl";e.left==null&&(e.left=r?e.end:e.start),e.right==null&&(e.right=r?e.start:e.end),e.bottom!=null&&(e.top=i.scrollHeight-i.clientHeight-e.bottom),r&&ws()!=wn.NORMAL?(e.left!=null&&(e.right=i.scrollWidth-i.clientWidth-e.left),ws()==wn.INVERTED?e.left=e.right:ws()==wn.NEGATED&&(e.left=e.right?-e.right:e.right)):e.right!=null&&(e.left=i.scrollWidth-i.clientWidth-e.right),this._applyScrollToOptions(e)}_applyScrollToOptions(e){let i=this.elementRef.nativeElement;Zu()?i.scrollTo(e):(e.top!=null&&(i.scrollTop=e.top),e.left!=null&&(i.scrollLeft=e.left))}measureScrollOffset(e){let i="left",r="right",o=this.elementRef.nativeElement;if(e=="top")return o.scrollTop;if(e=="bottom")return o.scrollHeight-o.clientHeight-o.scrollTop;let s=this.dir&&this.dir.value=="rtl";return e=="start"?e=s?r:i:e=="end"&&(e=s?i:r),s&&ws()==wn.INVERTED?e==i?o.scrollWidth-o.clientWidth-o.scrollLeft:o.scrollLeft:s&&ws()==wn.NEGATED?e==i?o.scrollLeft+o.scrollWidth-o.clientWidth:-o.scrollLeft:e==i?o.scrollLeft:o.scrollWidth-o.clientWidth-o.scrollLeft}static \u0275fac=function(i){return new(i||t)};static \u0275dir=z({type:t,selectors:[["","cdk-scrollable",""],["","cdkScrollable",""]]})}return t})(),yP=20,Ci=(()=>{class t{_platform=u(be);_listeners;_viewportSize=null;_change=new x;_document=u(j);constructor(){let e=u(k),i=u(Qe).createRenderer(null,null);e.runOutsideAngular(()=>{if(this._platform.isBrowser){let r=o=>this._change.next(o);this._listeners=[i.listen("window","resize",r),i.listen("window","orientationchange",r)]}this.change().subscribe(()=>this._viewportSize=null)})}ngOnDestroy(){this._listeners?.forEach(e=>e()),this._change.complete()}getViewportSize(){this._viewportSize||this._updateViewportSize();let e={width:this._viewportSize.width,height:this._viewportSize.height};return this._platform.isBrowser||(this._viewportSize=null),e}getViewportRect(){let e=this.getViewportScrollPosition(),{width:i,height:r}=this.getViewportSize();return{top:e.top,left:e.left,bottom:e.top+r,right:e.left+i,height:r,width:i}}getViewportScrollPosition(){if(!this._platform.isBrowser)return{top:0,left:0};let e=this._document,i=this._getWindow(),r=e.documentElement,o=r.getBoundingClientRect(),s=-o.top||e.body?.scrollTop||i.scrollY||r.scrollTop||0,a=-o.left||e.body?.scrollLeft||i.scrollX||r.scrollLeft||0;return{top:s,left:a}}change(e=yP){return e>0?this._change.pipe(Oc(e)):this._change}_getWindow(){return this._document.defaultView||window}_updateViewportSize(){let e=this._getWindow();this._viewportSize=this._platform.isBrowser?{width:e.innerWidth,height:e.innerHeight}:{width:0,height:0}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();var Si=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({})}return t})(),sv=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({imports:[ve,Si,ve,Si]})}return t})();var AD=new Map,Ie=class t{_appId=u(si);static _infix=`a${Math.floor(Math.random()*1e5).toString()}`;getId(n,e=!1){this._appId!=="ng"&&(n+=this._appId);let i=AD.get(n);return i===void 0?i=0:i++,AD.set(n,i),`${n}${e?t._infix+"-":""}${i}`}static \u0275fac=function(e){return new(e||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})};function $e(t,...n){return n.length?n.some(e=>t[e]):t.altKey||t.shiftKey||t.ctrlKey||t.metaKey}var RD=Zu();function or(t){return new Xu(t.get(Ci),t.get(j))}var Xu=class{_viewportRuler;_previousHTMLStyles={top:"",left:""};_previousScrollPosition;_isEnabled=!1;_document;constructor(n,e){this._viewportRuler=n,this._document=e}attach(){}enable(){if(this._canBeEnabled()){let n=this._document.documentElement;this._previousScrollPosition=this._viewportRuler.getViewportScrollPosition(),this._previousHTMLStyles.left=n.style.left||"",this._previousHTMLStyles.top=n.style.top||"",n.style.left=Xe(-this._previousScrollPosition.left),n.style.top=Xe(-this._previousScrollPosition.top),n.classList.add("cdk-global-scrollblock"),this._isEnabled=!0}}disable(){if(this._isEnabled){let n=this._document.documentElement,e=this._document.body,i=n.style,r=e.style,o=i.scrollBehavior||"",s=r.scrollBehavior||"";this._isEnabled=!1,i.left=this._previousHTMLStyles.left,i.top=this._previousHTMLStyles.top,n.classList.remove("cdk-global-scrollblock"),RD&&(i.scrollBehavior=r.scrollBehavior="auto"),window.scroll(this._previousScrollPosition.left,this._previousScrollPosition.top),RD&&(i.scrollBehavior=o,r.scrollBehavior=s)}}_canBeEnabled(){if(this._document.documentElement.classList.contains("cdk-global-scrollblock")||this._isEnabled)return!1;let e=this._document.documentElement,i=this._viewportRuler.getViewportSize();return e.scrollHeight>i.height||e.scrollWidth>i.width}};function jD(t,n){return new Ku(t.get(rr),t.get(k),t.get(Ci),n)}var Ku=class{_scrollDispatcher;_ngZone;_viewportRuler;_config;_scrollSubscription=null;_overlayRef;_initialScrollPosition;constructor(n,e,i,r){this._scrollDispatcher=n,this._ngZone=e,this._viewportRuler=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(this._scrollSubscription)return;let n=this._scrollDispatcher.scrolled(0).pipe(ge(e=>!e||!this._overlayRef.overlayElement.contains(e.getElementRef().nativeElement)));this._config&&this._config.threshold&&this._config.threshold>1?(this._initialScrollPosition=this._viewportRuler.getViewportScrollPosition().top,this._scrollSubscription=n.subscribe(()=>{let e=this._viewportRuler.getViewportScrollPosition().top;Math.abs(e-this._initialScrollPosition)>this._config.threshold?this._detach():this._overlayRef.updatePosition()})):this._scrollSubscription=n.subscribe(this._detach)}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}_detach=()=>{this.disable(),this._overlayRef.hasAttached()&&this._ngZone.run(()=>this._overlayRef.detach())}};var Ml=class{enable(){}disable(){}attach(){}};function lv(t,n){return n.some(e=>{let i=t.bottom<e.top,r=t.top>e.bottom,o=t.right<e.left,s=t.left>e.right;return i||r||o||s})}function OD(t,n){return n.some(e=>{let i=t.top<e.top,r=t.bottom>e.bottom,o=t.left<e.left,s=t.right>e.right;return i||r||o||s})}function Di(t,n){return new Qu(t.get(rr),t.get(Ci),t.get(k),n)}var Qu=class{_scrollDispatcher;_viewportRuler;_ngZone;_config;_scrollSubscription=null;_overlayRef;constructor(n,e,i,r){this._scrollDispatcher=n,this._viewportRuler=e,this._ngZone=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(!this._scrollSubscription){let n=this._config?this._config.scrollThrottle:0;this._scrollSubscription=this._scrollDispatcher.scrolled(n).subscribe(()=>{if(this._overlayRef.updatePosition(),this._config&&this._config.autoClose){let e=this._overlayRef.overlayElement.getBoundingClientRect(),{width:i,height:r}=this._viewportRuler.getViewportSize();lv(e,[{width:i,height:r,bottom:r,right:i,top:0,left:0}])&&(this.disable(),this._ngZone.run(()=>this._overlayRef.detach()))}})}}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}},HD=(()=>{class t{_injector=u(R);noop=()=>new Ml;close=e=>jD(this._injector,e);block=()=>or(this._injector);reposition=e=>Di(this._injector,e);static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})(),In=class{positionStrategy;scrollStrategy=new Ml;panelClass="";hasBackdrop=!1;backdropClass="cdk-overlay-dark-backdrop";disableAnimations;width;height;minWidth;minHeight;maxWidth;maxHeight;direction;disposeOnNavigation=!1;usePopover;eventPredicate;constructor(n){if(n){let e=Object.keys(n);for(let i of e)n[i]!==void 0&&(this[i]=n[i])}}};var Ju=class{connectionPair;scrollableViewProperties;constructor(n,e){this.connectionPair=n,this.scrollableViewProperties=e}};var UD=(()=>{class t{_attachedOverlays=[];_document=u(j);_isAttached=!1;ngOnDestroy(){this.detach()}add(e){this.remove(e),this._attachedOverlays.push(e)}remove(e){let i=this._attachedOverlays.indexOf(e);i>-1&&this._attachedOverlays.splice(i,1),this._attachedOverlays.length===0&&this.detach()}canReceiveEvent(e,i,r){return r.observers.length<1?!1:e.eventPredicate?e.eventPredicate(i):!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})(),zD=(()=>{class t extends UD{_ngZone=u(k);_renderer=u(Qe).createRenderer(null,null);_cleanupKeydown;add(e){super.add(e),this._isAttached||(this._ngZone.runOutsideAngular(()=>{this._cleanupKeydown=this._renderer.listen("body","keydown",this._keydownListener)}),this._isAttached=!0)}detach(){this._isAttached&&(this._cleanupKeydown?.(),this._isAttached=!1)}_keydownListener=e=>{let i=this._attachedOverlays;for(let r=i.length-1;r>-1;r--){let o=i[r];if(this.canReceiveEvent(o,e,o._keydownEvents)){this._ngZone.run(()=>o._keydownEvents.next(e));break}}};static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})(),$D=(()=>{class t extends UD{_platform=u(be);_ngZone=u(k);_renderer=u(Qe).createRenderer(null,null);_cursorOriginalValue;_cursorStyleIsSet=!1;_pointerDownEventTarget=null;_cleanups;add(e){if(super.add(e),!this._isAttached){let i=this._document.body,r={capture:!0},o=this._renderer;this._cleanups=this._ngZone.runOutsideAngular(()=>[o.listen(i,"pointerdown",this._pointerDownListener,r),o.listen(i,"click",this._clickListener,r),o.listen(i,"auxclick",this._clickListener,r),o.listen(i,"contextmenu",this._clickListener,r)]),this._platform.IOS&&!this._cursorStyleIsSet&&(this._cursorOriginalValue=i.style.cursor,i.style.cursor="pointer",this._cursorStyleIsSet=!0),this._isAttached=!0}}detach(){this._isAttached&&(this._cleanups?.forEach(e=>e()),this._cleanups=void 0,this._platform.IOS&&this._cursorStyleIsSet&&(this._document.body.style.cursor=this._cursorOriginalValue,this._cursorStyleIsSet=!1),this._isAttached=!1)}_pointerDownListener=e=>{this._pointerDownEventTarget=yt(e)};_clickListener=e=>{let i=yt(e),r=e.type==="click"&&this._pointerDownEventTarget?this._pointerDownEventTarget:i;this._pointerDownEventTarget=null;let o=this._attachedOverlays.slice();for(let s=o.length-1;s>-1;s--){let a=o[s],l=a._outsidePointerEvents;if(!(!a.hasAttached()||!this.canReceiveEvent(a,e,l))){if(PD(a.overlayElement,i)||PD(a.overlayElement,r))break;this._ngZone?this._ngZone.run(()=>l.next(e)):l.next(e)}}};static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();function PD(t,n){let e=typeof ShadowRoot<"u"&&ShadowRoot,i=n;for(;i;){if(i===t)return!0;i=e&&i instanceof ShadowRoot?i.host:i.parentNode}return!1}var GD=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-overlay-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-overlay-container, .cdk-global-overlay-wrapper {
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
`],encapsulation:2})}return t})(),nf=(()=>{class t{_platform=u(be);_containerElement;_document=u(j);_styleLoader=u(ze);ngOnDestroy(){this._containerElement?.remove()}getContainerElement(){return this._loadStyles(),this._containerElement||this._createContainer(),this._containerElement}_createContainer(){let e="cdk-overlay-container";if(this._platform.isBrowser||rv()){let r=this._document.querySelectorAll(`.${e}[platform="server"], .${e}[platform="test"]`);for(let o=0;o<r.length;o++)r[o].remove()}let i=this._document.createElement("div");i.classList.add(e),rv()?i.setAttribute("platform","test"):this._platform.isBrowser||i.setAttribute("platform","server"),this._document.body.appendChild(i),this._containerElement=i}_loadStyles(){this._styleLoader.load(GD)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})(),cv=class{_renderer;_ngZone;element;_cleanupClick;_cleanupTransitionEnd;_fallbackTimeout;constructor(n,e,i,r){this._renderer=e,this._ngZone=i,this.element=n.createElement("div"),this.element.classList.add("cdk-overlay-backdrop"),this._cleanupClick=e.listen(this.element,"click",r)}detach(){this._ngZone.runOutsideAngular(()=>{let n=this.element;clearTimeout(this._fallbackTimeout),this._cleanupTransitionEnd?.(),this._cleanupTransitionEnd=this._renderer.listen(n,"transitionend",this.dispose),this._fallbackTimeout=setTimeout(this.dispose,500),n.style.pointerEvents="none",n.classList.remove("cdk-overlay-backdrop-showing")})}dispose=()=>{clearTimeout(this._fallbackTimeout),this._cleanupClick?.(),this._cleanupTransitionEnd?.(),this._cleanupClick=this._cleanupTransitionEnd=this._fallbackTimeout=void 0,this.element.remove()}};function dv(t){return t&&t.nodeType===1}var av=new Set;var Is=class{_portalOutlet;_host;_pane;_config;_ngZone;_keyboardDispatcher;_document;_location;_outsideClickDispatcher;_animationsDisabled;_injector;_renderer;_backdropClick=new x;_attachments=new x;_detachments=new x;_positionStrategy;_scrollStrategy;_locationChanges=pe.EMPTY;_backdropRef=null;_detachContentMutationObserver;_detachContentAfterRenderRef;_disposed=!1;_previousHostParent;_keydownEvents=new x;_outsidePointerEvents=new x;_afterNextRenderRef;constructor(n,e,i,r,o,s,a,l,c,d=!1,f,h){this._portalOutlet=n,this._host=e,this._pane=i,this._config=r,this._ngZone=o,this._keyboardDispatcher=s,this._document=a,this._location=l,this._outsideClickDispatcher=c,this._animationsDisabled=d,this._injector=f,this._renderer=h,r.scrollStrategy&&(this._scrollStrategy=r.scrollStrategy,this._scrollStrategy.attach(this)),this._positionStrategy=r.positionStrategy}get overlayElement(){return this._pane}get backdropElement(){return this._backdropRef?.element||null}get hostElement(){return this._host}get eventPredicate(){return this._config?.eventPredicate||null}attach(n){if(this._disposed)return null;this._attachHost();let e=this._portalOutlet.attach(n);return this._positionStrategy?.attach(this),this._updateStackingOrder(),this._updateElementSize(),this._updateElementDirection(),av.add(this),this._scrollStrategy&&this._scrollStrategy.enable(),this._afterNextRenderRef?.destroy(),this._afterNextRenderRef=Je(()=>{this.hasAttached()&&this.updatePosition()},{injector:this._injector}),this._togglePointerEvents(!0),this._config.hasBackdrop&&this._attachBackdrop(),this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!0),this._attachments.next(),this._completeDetachContent(),this._keyboardDispatcher.add(this),this._config.disposeOnNavigation&&(this._locationChanges=this._location.subscribe(()=>this.dispose())),this._outsideClickDispatcher.add(this),typeof e?.onDestroy=="function"&&e.onDestroy(()=>{this.hasAttached()&&this._ngZone.runOutsideAngular(()=>Promise.resolve().then(()=>this.detach()))}),e}detach(){if(!this.hasAttached())return;this.detachBackdrop(),this._togglePointerEvents(!1),this._positionStrategy&&this._positionStrategy.detach&&this._positionStrategy.detach(),this._scrollStrategy&&this._scrollStrategy.disable();let n=this._portalOutlet.detach();return this._detachments.next(),this._completeDetachContent(),this._keyboardDispatcher.remove(this),this._detachContentWhenEmpty(),this._locationChanges.unsubscribe(),this._outsideClickDispatcher.remove(this),av.delete(this),n}dispose(){if(this._disposed)return;let n=this.hasAttached();this._positionStrategy&&this._positionStrategy.dispose(),this._disposeScrollStrategy(),this._backdropRef?.dispose(),this._locationChanges.unsubscribe(),this._keyboardDispatcher.remove(this),this._portalOutlet.dispose(),this._attachments.complete(),this._backdropClick.complete(),this._keydownEvents.complete(),this._outsidePointerEvents.complete(),this._outsideClickDispatcher.remove(this),this._host?.remove(),this._afterNextRenderRef?.destroy(),this._previousHostParent=this._pane=this._host=this._backdropRef=null,n&&this._detachments.next(),this._detachments.complete(),this._completeDetachContent(),this._disposed=!0,av.delete(this)}hasAttached(){return this._portalOutlet.hasAttached()}backdropClick(){return this._backdropClick}attachments(){return this._attachments}detachments(){return this._detachments}keydownEvents(){return this._keydownEvents}outsidePointerEvents(){return this._outsidePointerEvents}getConfig(){return this._config}updatePosition(){this._positionStrategy&&this._positionStrategy.apply()}updatePositionStrategy(n){n!==this._positionStrategy&&(this._positionStrategy&&this._positionStrategy.dispose(),this._positionStrategy=n,this.hasAttached()&&(n.attach(this),this.updatePosition()))}updateSize(n){this._config=v(v({},this._config),n),this._updateElementSize()}setDirection(n){this._config=N(v({},this._config),{direction:n}),this._updateElementDirection()}addPanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!0)}removePanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!1)}getDirection(){let n=this._config.direction;return n?typeof n=="string"?n:n.value:"ltr"}updateScrollStrategy(n){n!==this._scrollStrategy&&(this._disposeScrollStrategy(),this._scrollStrategy=n,this.hasAttached()&&(n.attach(this),n.enable()))}_updateElementDirection(){this._host.setAttribute("dir",this.getDirection())}_updateElementSize(){if(!this._pane)return;let n=this._pane.style;n.width=Xe(this._config.width),n.height=Xe(this._config.height),n.minWidth=Xe(this._config.minWidth),n.minHeight=Xe(this._config.minHeight),n.maxWidth=Xe(this._config.maxWidth),n.maxHeight=Xe(this._config.maxHeight)}_togglePointerEvents(n){this._pane.style.pointerEvents=n?"":"none"}_attachHost(){if(!this._host.parentElement){let n=this._config.usePopover?this._positionStrategy?.getPopoverInsertionPoint?.():null;dv(n)?n.after(this._host):n?.type==="parent"?n.element.appendChild(this._host):this._previousHostParent?.appendChild(this._host)}if(this._config.usePopover)try{this._host.showPopover()}catch{}}_attachBackdrop(){let n="cdk-overlay-backdrop-showing";this._backdropRef?.dispose(),this._backdropRef=new cv(this._document,this._renderer,this._ngZone,e=>{this._backdropClick.next(e)}),this._animationsDisabled&&this._backdropRef.element.classList.add("cdk-overlay-backdrop-noop-animation"),this._config.backdropClass&&this._toggleClasses(this._backdropRef.element,this._config.backdropClass,!0),this._config.usePopover?this._host.prepend(this._backdropRef.element):this._host.parentElement.insertBefore(this._backdropRef.element,this._host),!this._animationsDisabled&&typeof requestAnimationFrame<"u"?this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>this._backdropRef?.element.classList.add(n))}):this._backdropRef.element.classList.add(n)}_updateStackingOrder(){!this._config.usePopover&&this._host.nextSibling&&this._host.parentNode.appendChild(this._host)}detachBackdrop(){this._animationsDisabled?(this._backdropRef?.dispose(),this._backdropRef=null):this._backdropRef?.detach()}_toggleClasses(n,e,i){let r=Es(e||[]).filter(o=>!!o);r.length&&(i?n.classList.add(...r):n.classList.remove(...r))}_detachContentWhenEmpty(){let n=!1;try{this._detachContentAfterRenderRef=Je(()=>{n=!0,this._detachContent()},{injector:this._injector})}catch(e){if(n)throw e;this._detachContent()}globalThis.MutationObserver&&this._pane&&(this._detachContentMutationObserver||=new globalThis.MutationObserver(()=>{this._detachContent()}),this._detachContentMutationObserver.observe(this._pane,{childList:!0}))}_detachContent(){(!this._pane||!this._host||this._pane.children.length===0)&&(this._pane&&this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!1),this._host&&this._host.parentElement&&(this._previousHostParent=this._host.parentElement,this._host.remove()),this._completeDetachContent())}_completeDetachContent(){this._detachContentAfterRenderRef?.destroy(),this._detachContentAfterRenderRef=void 0,this._detachContentMutationObserver?.disconnect()}_disposeScrollStrategy(){let n=this._scrollStrategy;n?.disable(),n?.detach?.()}},FD="cdk-overlay-connected-position-bounding-box",CP=/([A-Za-z%]+)$/;function go(t,n){return new ef(n,t.get(Ci),t.get(j),t.get(be),t.get(nf))}var ef=class{_viewportRuler;_document;_platform;_overlayContainer;_overlayRef;_isInitialRender=!1;_lastBoundingBoxSize={width:0,height:0};_isPushed=!1;_canPush=!0;_growAfterOpen=!1;_hasFlexibleDimensions=!0;_positionLocked=!1;_originRect;_overlayRect;_viewportRect;_containerRect;_viewportMargin=0;_scrollables=[];_preferredPositions=[];_origin;_pane;_isDisposed=!1;_boundingBox=null;_lastPosition=null;_lastScrollVisibility=null;_positionChanges=new x;_resizeSubscription=pe.EMPTY;_offsetX=0;_offsetY=0;_transformOriginSelector;_appliedPanelClasses=[];_previousPushAmount=null;_popoverLocation="global";positionChanges=this._positionChanges;get positions(){return this._preferredPositions}constructor(n,e,i,r,o){this._viewportRuler=e,this._document=i,this._platform=r,this._overlayContainer=o,this.setOrigin(n)}attach(n){this._overlayRef&&this._overlayRef,this._validatePositions(),n.hostElement.classList.add(FD),this._overlayRef=n,this._boundingBox=n.hostElement,this._pane=n.overlayElement,this._isDisposed=!1,this._isInitialRender=!0,this._lastPosition=null,this._resizeSubscription.unsubscribe(),this._resizeSubscription=this._viewportRuler.change().subscribe(()=>{this._isInitialRender=!0,this.apply()})}apply(){if(this._isDisposed||!this._platform.isBrowser)return;if(!this._isInitialRender&&this._positionLocked&&this._lastPosition){this.reapplyLastPosition();return}this._clearPanelClasses(),this._resetOverlayElementStyles(),this._resetBoundingBoxStyles(),this._viewportRect=this._getNarrowedViewportRect(),this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._containerRect=this._getContainerRect();let n=this._originRect,e=this._overlayRect,i=this._viewportRect,r=this._containerRect,o=[],s;for(let a of this._preferredPositions){let l=this._getOriginPoint(n,r,a),c=this._getOverlayPoint(l,e,a),d=this._getOverlayFit(c,e,i,a);if(d.isCompletelyWithinViewport){this._isPushed=!1,this._applyPosition(a,l);return}if(this._canFitWithFlexibleDimensions(d,c,i)){o.push({position:a,origin:l,overlayRect:e,boundingBoxRect:this._calculateBoundingBoxRect(l,a)});continue}(!s||s.overlayFit.visibleArea<d.visibleArea)&&(s={overlayFit:d,overlayPoint:c,originPoint:l,position:a,overlayRect:e})}if(o.length){let a=null,l=-1;for(let c of o){let d=c.boundingBoxRect.width*c.boundingBoxRect.height*(c.position.weight||1);d>l&&(l=d,a=c)}this._isPushed=!1,this._applyPosition(a.position,a.origin);return}if(this._canPush){this._isPushed=!0,this._applyPosition(s.position,s.originPoint);return}this._applyPosition(s.position,s.originPoint)}detach(){this._clearPanelClasses(),this._lastPosition=null,this._previousPushAmount=null,this._resizeSubscription.unsubscribe()}dispose(){this._isDisposed||(this._boundingBox&&po(this._boundingBox.style,{top:"",left:"",right:"",bottom:"",height:"",width:"",alignItems:"",justifyContent:""}),this._pane&&this._resetOverlayElementStyles(),this._overlayRef&&this._overlayRef.hostElement.classList.remove(FD),this.detach(),this._positionChanges.complete(),this._overlayRef=this._boundingBox=null,this._isDisposed=!0)}reapplyLastPosition(){if(this._isDisposed||!this._platform.isBrowser)return;let n=this._lastPosition;n?(this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._viewportRect=this._getNarrowedViewportRect(),this._containerRect=this._getContainerRect(),this._applyPosition(n,this._getOriginPoint(this._originRect,this._containerRect,n))):this.apply()}withScrollableContainers(n){return this._scrollables=n,this}withPositions(n){return this._preferredPositions=n,n.indexOf(this._lastPosition)===-1&&(this._lastPosition=null),this._validatePositions(),this}withViewportMargin(n){return this._viewportMargin=n,this}withFlexibleDimensions(n=!0){return this._hasFlexibleDimensions=n,this}withGrowAfterOpen(n=!0){return this._growAfterOpen=n,this}withPush(n=!0){return this._canPush=n,this}withLockedPosition(n=!0){return this._positionLocked=n,this}setOrigin(n){return this._origin=n,this}withDefaultOffsetX(n){return this._offsetX=n,this}withDefaultOffsetY(n){return this._offsetY=n,this}withTransformOriginOn(n){return this._transformOriginSelector=n,this}withPopoverLocation(n){return this._popoverLocation=n,this}getPopoverInsertionPoint(){return this._popoverLocation==="global"?null:this._popoverLocation!=="inline"?this._popoverLocation:this._origin instanceof O?this._origin.nativeElement:dv(this._origin)?this._origin:null}_getOriginPoint(n,e,i){let r;if(i.originX=="center")r=n.left+n.width/2;else{let s=this._isRtl()?n.right:n.left,a=this._isRtl()?n.left:n.right;r=i.originX=="start"?s:a}e.left<0&&(r-=e.left);let o;return i.originY=="center"?o=n.top+n.height/2:o=i.originY=="top"?n.top:n.bottom,e.top<0&&(o-=e.top),{x:r,y:o}}_getOverlayPoint(n,e,i){let r;i.overlayX=="center"?r=-e.width/2:i.overlayX==="start"?r=this._isRtl()?-e.width:0:r=this._isRtl()?0:-e.width;let o;return i.overlayY=="center"?o=-e.height/2:o=i.overlayY=="top"?0:-e.height,{x:n.x+r,y:n.y+o}}_getOverlayFit(n,e,i,r){let o=VD(e),{x:s,y:a}=n,l=this._getOffset(r,"x"),c=this._getOffset(r,"y");l&&(s+=l),c&&(a+=c);let d=0-s,f=s+o.width-i.width,h=0-a,p=a+o.height-i.height,S=this._subtractOverflows(o.width,d,f),E=this._subtractOverflows(o.height,h,p),$=S*E;return{visibleArea:$,isCompletelyWithinViewport:o.width*o.height===$,fitsInViewportVertically:E===o.height,fitsInViewportHorizontally:S==o.width}}_canFitWithFlexibleDimensions(n,e,i){if(this._hasFlexibleDimensions){let r=i.bottom-e.y,o=i.right-e.x,s=LD(this._overlayRef.getConfig().minHeight),a=LD(this._overlayRef.getConfig().minWidth),l=n.fitsInViewportVertically||s!=null&&s<=r,c=n.fitsInViewportHorizontally||a!=null&&a<=o;return l&&c}return!1}_pushOverlayOnScreen(n,e,i){if(this._previousPushAmount&&this._positionLocked)return{x:n.x+this._previousPushAmount.x,y:n.y+this._previousPushAmount.y};let r=VD(e),o=this._viewportRect,s=Math.max(n.x+r.width-o.width,0),a=Math.max(n.y+r.height-o.height,0),l=Math.max(o.top-i.top-n.y,0),c=Math.max(o.left-i.left-n.x,0),d=0,f=0;return r.width<=o.width?d=c||-s:d=n.x<this._getViewportMarginStart()?o.left-i.left-n.x:0,r.height<=o.height?f=l||-a:f=n.y<this._getViewportMarginTop()?o.top-i.top-n.y:0,this._previousPushAmount={x:d,y:f},{x:n.x+d,y:n.y+f}}_applyPosition(n,e){if(this._setTransformOrigin(n),this._setOverlayElementStyles(e,n),this._setBoundingBoxStyles(e,n),n.panelClass&&this._addPanelClasses(n.panelClass),this._positionChanges.observers.length){let i=this._getScrollVisibility();if(n!==this._lastPosition||!this._lastScrollVisibility||!DP(this._lastScrollVisibility,i)){let r=new Ju(n,i);this._positionChanges.next(r)}this._lastScrollVisibility=i}this._lastPosition=n,this._isInitialRender=!1}_setTransformOrigin(n){if(!this._transformOriginSelector)return;let e=this._boundingBox.querySelectorAll(this._transformOriginSelector),i,r=n.overlayY;n.overlayX==="center"?i="center":this._isRtl()?i=n.overlayX==="start"?"right":"left":i=n.overlayX==="start"?"left":"right";for(let o=0;o<e.length;o++)e[o].style.transformOrigin=`${i} ${r}`}_calculateBoundingBoxRect(n,e){let i=this._viewportRect,r=this._isRtl(),o,s,a;if(e.overlayY==="top")s=n.y,o=i.height-s+this._getViewportMarginBottom();else if(e.overlayY==="bottom")a=i.height-n.y+this._getViewportMarginTop()+this._getViewportMarginBottom(),o=i.height-a+this._getViewportMarginTop();else{let p=Math.min(i.bottom-n.y+i.top,n.y),S=this._lastBoundingBoxSize.height;o=p*2,s=n.y-p,o>S&&!this._isInitialRender&&!this._growAfterOpen&&(s=n.y-S/2)}let l=e.overlayX==="start"&&!r||e.overlayX==="end"&&r,c=e.overlayX==="end"&&!r||e.overlayX==="start"&&r,d,f,h;if(c)h=i.width-n.x+this._getViewportMarginStart()+this._getViewportMarginEnd(),d=n.x-this._getViewportMarginStart();else if(l)f=n.x,d=i.right-n.x-this._getViewportMarginEnd();else{let p=Math.min(i.right-n.x+i.left,n.x),S=this._lastBoundingBoxSize.width;d=p*2,f=n.x-p,d>S&&!this._isInitialRender&&!this._growAfterOpen&&(f=n.x-S/2)}return{top:s,left:f,bottom:a,right:h,width:d,height:o}}_setBoundingBoxStyles(n,e){let i=this._calculateBoundingBoxRect(n,e);!this._isInitialRender&&!this._growAfterOpen&&(i.height=Math.min(i.height,this._lastBoundingBoxSize.height),i.width=Math.min(i.width,this._lastBoundingBoxSize.width));let r={};if(this._hasExactPosition())r.top=r.left="0",r.bottom=r.right="auto",r.maxHeight=r.maxWidth="",r.width=r.height="100%";else{let o=this._overlayRef.getConfig().maxHeight,s=this._overlayRef.getConfig().maxWidth;r.width=Xe(i.width),r.height=Xe(i.height),r.top=Xe(i.top)||"auto",r.bottom=Xe(i.bottom)||"auto",r.left=Xe(i.left)||"auto",r.right=Xe(i.right)||"auto",e.overlayX==="center"?r.alignItems="center":r.alignItems=e.overlayX==="end"?"flex-end":"flex-start",e.overlayY==="center"?r.justifyContent="center":r.justifyContent=e.overlayY==="bottom"?"flex-end":"flex-start",o&&(r.maxHeight=Xe(o)),s&&(r.maxWidth=Xe(s))}this._lastBoundingBoxSize=i,po(this._boundingBox.style,r)}_resetBoundingBoxStyles(){po(this._boundingBox.style,{top:"0",left:"0",right:"0",bottom:"0",height:"",width:"",alignItems:"",justifyContent:""})}_resetOverlayElementStyles(){po(this._pane.style,{top:"",left:"",bottom:"",right:"",position:"",transform:""})}_setOverlayElementStyles(n,e){let i={},r=this._hasExactPosition(),o=this._hasFlexibleDimensions,s=this._overlayRef.getConfig();if(r){let d=this._viewportRuler.getViewportScrollPosition();po(i,this._getExactOverlayY(e,n,d)),po(i,this._getExactOverlayX(e,n,d))}else i.position="static";let a="",l=this._getOffset(e,"x"),c=this._getOffset(e,"y");l&&(a+=`translateX(${l}px) `),c&&(a+=`translateY(${c}px)`),i.transform=a.trim(),s.maxHeight&&(r?i.maxHeight=Xe(s.maxHeight):o&&(i.maxHeight="")),s.maxWidth&&(r?i.maxWidth=Xe(s.maxWidth):o&&(i.maxWidth="")),po(this._pane.style,i)}_getExactOverlayY(n,e,i){let r={top:"",bottom:""},o=this._getOverlayPoint(e,this._overlayRect,n);if(this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i)),n.overlayY==="bottom"){let s=this._document.documentElement.clientHeight;r.bottom=`${s-(o.y+this._overlayRect.height)}px`}else r.top=Xe(o.y);return r}_getExactOverlayX(n,e,i){let r={left:"",right:""},o=this._getOverlayPoint(e,this._overlayRect,n);this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i));let s;if(this._isRtl()?s=n.overlayX==="end"?"left":"right":s=n.overlayX==="end"?"right":"left",s==="right"){let a=this._document.documentElement.clientWidth;r.right=`${a-(o.x+this._overlayRect.width)}px`}else r.left=Xe(o.x);return r}_getScrollVisibility(){let n=this._getOriginRect(),e=this._pane.getBoundingClientRect(),i=this._scrollables.map(r=>r.getElementRef().nativeElement.getBoundingClientRect());return{isOriginClipped:OD(n,i),isOriginOutsideView:lv(n,i),isOverlayClipped:OD(e,i),isOverlayOutsideView:lv(e,i)}}_subtractOverflows(n,...e){return e.reduce((i,r)=>i-Math.max(r,0),n)}_getNarrowedViewportRect(){let n=this._document.documentElement.clientWidth,e=this._document.documentElement.clientHeight,i=this._viewportRuler.getViewportScrollPosition();return{top:i.top+this._getViewportMarginTop(),left:i.left+this._getViewportMarginStart(),right:i.left+n-this._getViewportMarginEnd(),bottom:i.top+e-this._getViewportMarginBottom(),width:n-this._getViewportMarginStart()-this._getViewportMarginEnd(),height:e-this._getViewportMarginTop()-this._getViewportMarginBottom()}}_isRtl(){return this._overlayRef.getDirection()==="rtl"}_hasExactPosition(){return!this._hasFlexibleDimensions||this._isPushed}_getOffset(n,e){return e==="x"?n.offsetX==null?this._offsetX:n.offsetX:n.offsetY==null?this._offsetY:n.offsetY}_validatePositions(){}_addPanelClasses(n){this._pane&&Es(n).forEach(e=>{e!==""&&this._appliedPanelClasses.indexOf(e)===-1&&(this._appliedPanelClasses.push(e),this._pane.classList.add(e))})}_clearPanelClasses(){this._pane&&(this._appliedPanelClasses.forEach(n=>{this._pane.classList.remove(n)}),this._appliedPanelClasses=[])}_getViewportMarginStart(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.start??0}_getViewportMarginEnd(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.end??0}_getViewportMarginTop(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.top??0}_getViewportMarginBottom(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.bottom??0}_getOriginRect(){let n=this._origin;if(n instanceof O)return n.nativeElement.getBoundingClientRect();if(n instanceof Element)return n.getBoundingClientRect();let e=n.width||0,i=n.height||0;return{top:n.y,bottom:n.y+i,left:n.x,right:n.x+e,height:i,width:e}}_getContainerRect(){let n=this._overlayRef.getConfig().usePopover&&this._popoverLocation!=="global",e=this._overlayContainer.getContainerElement();n&&(e.style.display="block");let i=e.getBoundingClientRect();return n&&(e.style.display=""),i}};function po(t,n){for(let e in n)n.hasOwnProperty(e)&&(t[e]=n[e]);return t}function LD(t){if(typeof t!="number"&&t!=null){let[n,e]=t.split(CP);return!e||e==="px"?parseFloat(n):null}return t||null}function VD(t){return{top:Math.floor(t.top),right:Math.floor(t.right),bottom:Math.floor(t.bottom),left:Math.floor(t.left),width:Math.floor(t.width),height:Math.floor(t.height)}}function DP(t,n){return t===n?!0:t.isOriginClipped===n.isOriginClipped&&t.isOriginOutsideView===n.isOriginOutsideView&&t.isOverlayClipped===n.isOverlayClipped&&t.isOverlayOutsideView===n.isOverlayOutsideView}var BD="cdk-global-overlay-wrapper";function Yn(t){return new tf}var tf=class{_overlayRef;_cssPosition="static";_topOffset="";_bottomOffset="";_alignItems="";_xPosition="";_xOffset="";_width="";_height="";_isDisposed=!1;attach(n){let e=n.getConfig();this._overlayRef=n,this._width&&!e.width&&n.updateSize({width:this._width}),this._height&&!e.height&&n.updateSize({height:this._height}),n.hostElement.classList.add(BD),this._isDisposed=!1}top(n=""){return this._bottomOffset="",this._topOffset=n,this._alignItems="flex-start",this}left(n=""){return this._xOffset=n,this._xPosition="left",this}bottom(n=""){return this._topOffset="",this._bottomOffset=n,this._alignItems="flex-end",this}right(n=""){return this._xOffset=n,this._xPosition="right",this}start(n=""){return this._xOffset=n,this._xPosition="start",this}end(n=""){return this._xOffset=n,this._xPosition="end",this}width(n=""){return this._overlayRef?this._overlayRef.updateSize({width:n}):this._width=n,this}height(n=""){return this._overlayRef?this._overlayRef.updateSize({height:n}):this._height=n,this}centerHorizontally(n=""){return this.left(n),this._xPosition="center",this}centerVertically(n=""){return this.top(n),this._alignItems="center",this}apply(){if(!this._overlayRef||!this._overlayRef.hasAttached())return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement.style,i=this._overlayRef.getConfig(),{width:r,height:o,maxWidth:s,maxHeight:a}=i,l=(r==="100%"||r==="100vw")&&(!s||s==="100%"||s==="100vw"),c=(o==="100%"||o==="100vh")&&(!a||a==="100%"||a==="100vh"),d=this._xPosition,f=this._xOffset,h=this._overlayRef.getConfig().direction==="rtl",p="",S="",E="";l?E="flex-start":d==="center"?(E="center",h?S=f:p=f):h?d==="left"||d==="end"?(E="flex-end",p=f):(d==="right"||d==="start")&&(E="flex-start",S=f):d==="left"||d==="start"?(E="flex-start",p=f):(d==="right"||d==="end")&&(E="flex-end",S=f),n.position=this._cssPosition,n.marginLeft=l?"0":p,n.marginTop=c?"0":this._topOffset,n.marginBottom=this._bottomOffset,n.marginRight=l?"0":S,e.justifyContent=E,e.alignItems=c?"flex-start":this._alignItems}dispose(){if(this._isDisposed||!this._overlayRef)return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement,i=e.style;e.classList.remove(BD),i.justifyContent=i.alignItems=n.marginTop=n.marginBottom=n.marginLeft=n.marginRight=n.position="",this._overlayRef=null,this._isDisposed=!0}},WD=(()=>{class t{_injector=u(R);global(){return Yn()}flexibleConnectedTo(e){return go(this._injector,e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})(),Tl=new b("OVERLAY_DEFAULT_CONFIG");function Tn(t,n){t.get(ze).load(GD);let e=t.get(nf),i=t.get(j),r=t.get(Ie),o=t.get(Ft),s=t.get(ct),a=t.get(Re,null,{optional:!0})||t.get(Qe).createRenderer(null,null),l=new In(n),c=t.get(Tl,null,{optional:!0})?.usePopover??!0;l.direction=l.direction||s.value,!i.body||!("showPopover"in i.body)?l.usePopover=!1:l.usePopover=n?.usePopover??c;let d=i.createElement("div"),f=i.createElement("div");d.id=r.getId("cdk-overlay-"),d.classList.add("cdk-overlay-pane"),f.appendChild(d),l.usePopover&&(f.setAttribute("popover","manual"),f.classList.add("cdk-overlay-popover"));let h=l.usePopover?l.positionStrategy?.getPopoverInsertionPoint?.():null;return dv(h)?h.after(f):h?.type==="parent"?h.element.appendChild(f):e.getContainerElement().appendChild(f),new Is(new Cl(d,o,t),f,d,l,t.get(k),t.get(zD),i,t.get(Zi),t.get($D),n?.disableAnimations??t.get(Ea,null,{optional:!0})==="NoopAnimations",t.get(Me),a)}var qD=(()=>{class t{scrollStrategies=u(HD);_positionBuilder=u(WD);_injector=u(R);create(e){return Tn(this._injector,e)}position(){return this._positionBuilder}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})(),EP=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"}],xP=new b("cdk-connected-overlay-scroll-strategy",{providedIn:"root",factory:()=>{let t=u(R);return()=>Di(t)}}),Ms=(()=>{class t{elementRef=u(O);static \u0275fac=function(i){return new(i||t)};static \u0275dir=z({type:t,selectors:[["","cdk-overlay-origin",""],["","overlay-origin",""],["","cdkOverlayOrigin",""]],exportAs:["cdkOverlayOrigin"]})}return t})(),YD=new b("cdk-connected-overlay-default-config"),rf=(()=>{class t{_dir=u(ct,{optional:!0});_injector=u(R);_overlayRef;_templatePortal;_backdropSubscription=pe.EMPTY;_attachSubscription=pe.EMPTY;_detachSubscription=pe.EMPTY;_positionSubscription=pe.EMPTY;_offsetX;_offsetY;_position;_scrollStrategyFactory=u(xP);_ngZone=u(k);origin;positions;positionStrategy;get offsetX(){return this._offsetX}set offsetX(e){this._offsetX=e,this._position&&this._updatePositionStrategy(this._position)}get offsetY(){return this._offsetY}set offsetY(e){this._offsetY=e,this._position&&this._updatePositionStrategy(this._position)}width;height;minWidth;minHeight;backdropClass;panelClass;viewportMargin=0;scrollStrategy;open=!1;disableClose=!1;transformOriginSelector;hasBackdrop=!1;lockPosition=!1;flexibleDimensions=!1;growAfterOpen=!1;push=!1;disposeOnNavigation=!1;usePopover;matchWidth=!1;set _config(e){typeof e!="string"&&this._assignConfig(e)}backdropClick=new W;positionChange=new W;attach=new W;detach=new W;overlayKeydown=new W;overlayOutsideClick=new W;constructor(){let e=u(Ct),i=u(bt),r=u(YD,{optional:!0}),o=u(Tl,{optional:!0});this.usePopover=o?.usePopover===!1?null:"global",this._templatePortal=new xn(e,i),this.scrollStrategy=this._scrollStrategyFactory(),r&&this._assignConfig(r)}get overlayRef(){return this._overlayRef}get dir(){return this._dir?this._dir.value:"ltr"}ngOnDestroy(){this._attachSubscription.unsubscribe(),this._detachSubscription.unsubscribe(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this._overlayRef?.dispose()}ngOnChanges(e){this._position&&(this._updatePositionStrategy(this._position),this._overlayRef?.updateSize({width:this._getWidth(),minWidth:this.minWidth,height:this.height,minHeight:this.minHeight}),e.origin&&this.open&&this._position.apply()),e.open&&(this.open?this.attachOverlay():this.detachOverlay())}_createOverlay(){(!this.positions||!this.positions.length)&&(this.positions=EP);let e=this._overlayRef=Tn(this._injector,this._buildConfig());this._attachSubscription=e.attachments().subscribe(()=>this.attach.emit()),this._detachSubscription=e.detachments().subscribe(()=>this.detach.emit()),e.keydownEvents().subscribe(i=>{this.overlayKeydown.next(i),i.keyCode===27&&!this.disableClose&&!$e(i)&&(i.preventDefault(),this.detachOverlay())}),this._overlayRef.outsidePointerEvents().subscribe(i=>{let r=this._getOriginElement(),o=yt(i);(!r||r!==o&&!r.contains(o))&&this.overlayOutsideClick.next(i)})}_buildConfig(){let e=this._position=this.positionStrategy||this._createPositionStrategy(),i=new In({direction:this._dir||"ltr",positionStrategy:e,scrollStrategy:this.scrollStrategy,hasBackdrop:this.hasBackdrop,disposeOnNavigation:this.disposeOnNavigation,usePopover:!!this.usePopover});return(this.height||this.height===0)&&(i.height=this.height),(this.minWidth||this.minWidth===0)&&(i.minWidth=this.minWidth),(this.minHeight||this.minHeight===0)&&(i.minHeight=this.minHeight),this.backdropClass&&(i.backdropClass=this.backdropClass),this.panelClass&&(i.panelClass=this.panelClass),i}_updatePositionStrategy(e){let i=this.positions.map(r=>({originX:r.originX,originY:r.originY,overlayX:r.overlayX,overlayY:r.overlayY,offsetX:r.offsetX||this.offsetX,offsetY:r.offsetY||this.offsetY,panelClass:r.panelClass||void 0}));return e.setOrigin(this._getOrigin()).withPositions(i).withFlexibleDimensions(this.flexibleDimensions).withPush(this.push).withGrowAfterOpen(this.growAfterOpen).withViewportMargin(this.viewportMargin).withLockedPosition(this.lockPosition).withTransformOriginOn(this.transformOriginSelector).withPopoverLocation(this.usePopover===null?"global":this.usePopover)}_createPositionStrategy(){let e=go(this._injector,this._getOrigin());return this._updatePositionStrategy(e),e}_getOrigin(){return this.origin instanceof Ms?this.origin.elementRef:this.origin}_getOriginElement(){return this.origin instanceof Ms?this.origin.elementRef.nativeElement:this.origin instanceof O?this.origin.nativeElement:typeof Element<"u"&&this.origin instanceof Element?this.origin:null}_getWidth(){return this.width?this.width:this.matchWidth?this._getOriginElement()?.getBoundingClientRect?.().width:void 0}attachOverlay(){this._overlayRef||this._createOverlay();let e=this._overlayRef;e.getConfig().hasBackdrop=this.hasBackdrop,e.updateSize({width:this._getWidth()}),e.hasAttached()||e.attach(this._templatePortal),this.hasBackdrop?this._backdropSubscription=e.backdropClick().subscribe(i=>this.backdropClick.emit(i)):this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.positionChange.observers.length>0&&(this._positionSubscription=this._position.positionChanges.pipe(hh(()=>this.positionChange.observers.length>0)).subscribe(i=>{this._ngZone.run(()=>this.positionChange.emit(i)),this.positionChange.observers.length===0&&this._positionSubscription.unsubscribe()})),this.open=!0}detachOverlay(){this._overlayRef?.detach(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.open=!1}_assignConfig(e){this.origin=e.origin??this.origin,this.positions=e.positions??this.positions,this.positionStrategy=e.positionStrategy??this.positionStrategy,this.offsetX=e.offsetX??this.offsetX,this.offsetY=e.offsetY??this.offsetY,this.width=e.width??this.width,this.height=e.height??this.height,this.minWidth=e.minWidth??this.minWidth,this.minHeight=e.minHeight??this.minHeight,this.backdropClass=e.backdropClass??this.backdropClass,this.panelClass=e.panelClass??this.panelClass,this.viewportMargin=e.viewportMargin??this.viewportMargin,this.scrollStrategy=e.scrollStrategy??this.scrollStrategy,this.disableClose=e.disableClose??this.disableClose,this.transformOriginSelector=e.transformOriginSelector??this.transformOriginSelector,this.hasBackdrop=e.hasBackdrop??this.hasBackdrop,this.lockPosition=e.lockPosition??this.lockPosition,this.flexibleDimensions=e.flexibleDimensions??this.flexibleDimensions,this.growAfterOpen=e.growAfterOpen??this.growAfterOpen,this.push=e.push??this.push,this.disposeOnNavigation=e.disposeOnNavigation??this.disposeOnNavigation,this.usePopover=e.usePopover??this.usePopover,this.matchWidth=e.matchWidth??this.matchWidth}static \u0275fac=function(i){return new(i||t)};static \u0275dir=z({type:t,selectors:[["","cdk-connected-overlay",""],["","connected-overlay",""],["","cdkConnectedOverlay",""]],inputs:{origin:[0,"cdkConnectedOverlayOrigin","origin"],positions:[0,"cdkConnectedOverlayPositions","positions"],positionStrategy:[0,"cdkConnectedOverlayPositionStrategy","positionStrategy"],offsetX:[0,"cdkConnectedOverlayOffsetX","offsetX"],offsetY:[0,"cdkConnectedOverlayOffsetY","offsetY"],width:[0,"cdkConnectedOverlayWidth","width"],height:[0,"cdkConnectedOverlayHeight","height"],minWidth:[0,"cdkConnectedOverlayMinWidth","minWidth"],minHeight:[0,"cdkConnectedOverlayMinHeight","minHeight"],backdropClass:[0,"cdkConnectedOverlayBackdropClass","backdropClass"],panelClass:[0,"cdkConnectedOverlayPanelClass","panelClass"],viewportMargin:[0,"cdkConnectedOverlayViewportMargin","viewportMargin"],scrollStrategy:[0,"cdkConnectedOverlayScrollStrategy","scrollStrategy"],open:[0,"cdkConnectedOverlayOpen","open"],disableClose:[0,"cdkConnectedOverlayDisableClose","disableClose"],transformOriginSelector:[0,"cdkConnectedOverlayTransformOriginOn","transformOriginSelector"],hasBackdrop:[2,"cdkConnectedOverlayHasBackdrop","hasBackdrop",V],lockPosition:[2,"cdkConnectedOverlayLockPosition","lockPosition",V],flexibleDimensions:[2,"cdkConnectedOverlayFlexibleDimensions","flexibleDimensions",V],growAfterOpen:[2,"cdkConnectedOverlayGrowAfterOpen","growAfterOpen",V],push:[2,"cdkConnectedOverlayPush","push",V],disposeOnNavigation:[2,"cdkConnectedOverlayDisposeOnNavigation","disposeOnNavigation",V],usePopover:[0,"cdkConnectedOverlayUsePopover","usePopover"],matchWidth:[2,"cdkConnectedOverlayMatchWidth","matchWidth",V],_config:[0,"cdkConnectedOverlay","_config"]},outputs:{backdropClick:"backdropClick",positionChange:"positionChange",attach:"attach",detach:"detach",overlayKeydown:"overlayKeydown",overlayOutsideClick:"overlayOutsideClick"},exportAs:["cdkConnectedOverlay"],features:[at]})}return t})(),an=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({providers:[qD],imports:[ve,_i,sv,sv]})}return t})();function wP(t,n){}var sr=class{viewContainerRef;injector;id;role="dialog";panelClass="";hasBackdrop=!0;backdropClass="";disableClose=!1;closePredicate;width="";height="";minWidth;minHeight;maxWidth;maxHeight;positionStrategy;data=null;direction;ariaDescribedBy=null;ariaLabelledBy=null;ariaLabel=null;ariaModal=!1;autoFocus="first-tabbable";restoreFocus=!0;scrollStrategy;closeOnNavigation=!0;closeOnDestroy=!0;closeOnOverlayDetachments=!0;disableAnimations=!1;providers;container;templateContext;bindings};var Nl=(()=>{class t extends nr{_elementRef=u(O);_focusTrapFactory=u(iv);_config;_interactivityChecker=u(nv);_ngZone=u(k);_focusMonitor=u(sn);_renderer=u(Re);_changeDetectorRef=u(Oe);_injector=u(R);_platform=u(be);_document=u(j);_portalOutlet;_focusTrapped=new x;_focusTrap=null;_elementFocusedBeforeDialogWasOpened=null;_closeInteractionType=null;_ariaLabelledByQueue=[];_isDestroyed=!1;constructor(){super(),this._config=u(sr,{optional:!0})||new sr,this._config.ariaLabelledBy&&this._ariaLabelledByQueue.push(this._config.ariaLabelledBy)}_addAriaLabelledBy(e){this._ariaLabelledByQueue.push(e),this._changeDetectorRef.markForCheck()}_removeAriaLabelledBy(e){let i=this._ariaLabelledByQueue.indexOf(e);i>-1&&(this._ariaLabelledByQueue.splice(i,1),this._changeDetectorRef.markForCheck())}_contentAttached(){this._initializeFocusTrap(),this._captureInitialFocus()}_captureInitialFocus(){this._trapFocus()}ngOnDestroy(){this._focusTrapped.complete(),this._isDestroyed=!0,this._restoreFocus()}attachComponentPortal(e){this._portalOutlet.hasAttached();let i=this._portalOutlet.attachComponentPortal(e);return this._contentAttached(),i}attachTemplatePortal(e){this._portalOutlet.hasAttached();let i=this._portalOutlet.attachTemplatePortal(e);return this._contentAttached(),i}attachDomPortal=e=>{this._portalOutlet.hasAttached();let i=this._portalOutlet.attachDomPortal(e);return this._contentAttached(),i};_recaptureFocus(){this._containsFocus()||this._trapFocus()}_forceFocus(e,i){this._interactivityChecker.isFocusable(e)||(e.tabIndex=-1,this._ngZone.runOutsideAngular(()=>{let r=()=>{o(),s(),e.removeAttribute("tabindex")},o=this._renderer.listen(e,"blur",r),s=this._renderer.listen(e,"mousedown",r)})),e.focus(i)}_focusByCssSelector(e,i){let r=this._elementRef.nativeElement.querySelector(e);r&&this._forceFocus(r,i)}_trapFocus(e){this._isDestroyed||Je(()=>{let i=this._elementRef.nativeElement;switch(this._config.autoFocus){case!1:case"dialog":this._containsFocus()||i.focus(e);break;case!0:case"first-tabbable":this._focusTrap?.focusInitialElement(e)||this._focusDialogContainer(e);break;case"first-heading":this._focusByCssSelector('h1, h2, h3, h4, h5, h6, [role="heading"]',e);break;default:this._focusByCssSelector(this._config.autoFocus,e);break}this._focusTrapped.next()},{injector:this._injector})}_restoreFocus(){let e=this._config.restoreFocus,i=null;if(typeof e=="string"?i=this._document.querySelector(e):typeof e=="boolean"?i=e?this._elementFocusedBeforeDialogWasOpened:null:e&&(i=e),this._config.restoreFocus&&i&&typeof i.focus=="function"){let r=Dl(),o=this._elementRef.nativeElement;(!r||r===this._document.body||r===o||o.contains(r))&&(this._focusMonitor?(this._focusMonitor.focusVia(i,this._closeInteractionType),this._closeInteractionType=null):i.focus())}this._focusTrap&&this._focusTrap.destroy()}_focusDialogContainer(e){this._elementRef.nativeElement.focus?.(e)}_containsFocus(){let e=this._elementRef.nativeElement,i=Dl();return e===i||e.contains(i)}_initializeFocusTrap(){this._platform.isBrowser&&(this._focusTrap=this._focusTrapFactory.create(this._elementRef.nativeElement),this._document&&(this._elementFocusedBeforeDialogWasOpened=Dl()))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["cdk-dialog-container"]],viewQuery:function(i,r){if(i&1&&Ue(qn,7),i&2){let o;ce(o=de())&&(r._portalOutlet=o.first)}},hostAttrs:["tabindex","-1",1,"cdk-dialog-container"],hostVars:6,hostBindings:function(i,r){i&2&&re("id",r._config.id||null)("role",r._config.role)("aria-modal",r._config.ariaModal)("aria-labelledby",r._config.ariaLabel?null:r._ariaLabelledByQueue[0])("aria-label",r._config.ariaLabel)("aria-describedby",r._config.ariaDescribedBy||null)},features:[Te],decls:1,vars:0,consts:[["cdkPortalOutlet",""]],template:function(i,r){i&1&&Dt(0,wP,0,0,"ng-template",0)},dependencies:[qn],styles:[`.cdk-dialog-container {
  display: block;
  width: 100%;
  height: 100%;
  min-height: inherit;
  max-height: inherit;
}
`],encapsulation:2,changeDetection:1})}return t})(),vo=class{overlayRef;config;componentInstance=null;componentRef=null;containerInstance;disableClose;closed=new x;backdropClick;keydownEvents;outsidePointerEvents;id;_detachSubscription;constructor(n,e){this.overlayRef=n,this.config=e,this.disableClose=e.disableClose,this.backdropClick=n.backdropClick(),this.keydownEvents=n.keydownEvents(),this.outsidePointerEvents=n.outsidePointerEvents(),this.id=e.id,this.keydownEvents.subscribe(i=>{i.keyCode===27&&!this.disableClose&&!$e(i)&&(i.preventDefault(),this.close(void 0,{focusOrigin:"keyboard"}))}),this.backdropClick.subscribe(()=>{!this.disableClose&&this._canClose()?this.close(void 0,{focusOrigin:"mouse"}):this.containerInstance._recaptureFocus?.()}),this._detachSubscription=n.detachments().subscribe(()=>{e.closeOnOverlayDetachments!==!1&&this.close()})}close(n,e){if(this._canClose(n)){let i=this.closed;this.containerInstance._closeInteractionType=e?.focusOrigin||"program",this._detachSubscription.unsubscribe(),this.overlayRef.dispose(),i.next(n),i.complete(),this.componentInstance=this.containerInstance=null}}updatePosition(){return this.overlayRef.updatePosition(),this}updateSize(n="",e=""){return this.overlayRef.updateSize({width:n,height:e}),this}addPanelClass(n){return this.overlayRef.addPanelClass(n),this}removePanelClass(n){return this.overlayRef.removePanelClass(n),this}_canClose(n){let e=this.config;return!!this.containerInstance&&(!e.closePredicate||e.closePredicate(n,e,this.componentInstance))}},IP=new b("DialogScrollStrategy",{providedIn:"root",factory:()=>{let t=u(R);return()=>or(t)}}),MP=new b("DialogData"),TP=new b("DefaultDialogConfig");function NP(t){let n=U(t),e=new W;return{valueSignal:n,get value(){return n()},change:e,ngOnDestroy(){e.complete()}}}var kl=(()=>{class t{_injector=u(R);_defaultOptions=u(TP,{optional:!0});_parentDialog=u(t,{optional:!0,skipSelf:!0});_overlayContainer=u(nf);_idGenerator=u(Ie);_openDialogsAtThisLevel=[];_afterAllClosedAtThisLevel=new x;_afterOpenedAtThisLevel=new x;_ariaHiddenElements=new Map;_scrollStrategy=u(IP);get openDialogs(){return this._parentDialog?this._parentDialog.openDialogs:this._openDialogsAtThisLevel}get afterOpened(){return this._parentDialog?this._parentDialog.afterOpened:this._afterOpenedAtThisLevel}afterAllClosed=hn(()=>this.openDialogs.length?this._getAfterAllClosed():this._getAfterAllClosed().pipe(nt(void 0)));open(e,i){let r=this._defaultOptions||new sr;i=v(v({},r),i),i.id=i.id||this._idGenerator.getId("cdk-dialog-"),i.id&&this.getDialogById(i.id);let o=this._getOverlayConfig(i),s=Tn(this._injector,o),a=new vo(s,i),l=this._attachContainer(s,a,i);if(a.containerInstance=l,!this.openDialogs.length){let c=this._overlayContainer.getContainerElement();l._focusTrapped?l._focusTrapped.pipe(Ae(1)).subscribe(()=>{this._hideNonDialogContentFromAssistiveTechnology(c)}):this._hideNonDialogContentFromAssistiveTechnology(c)}return this._attachDialogContent(e,a,l,i),this.openDialogs.push(a),a.closed.subscribe(()=>this._removeOpenDialog(a,!0)),this.afterOpened.next(a),a}closeAll(){uv(this.openDialogs,e=>e.close())}getDialogById(e){return this.openDialogs.find(i=>i.id===e)}ngOnDestroy(){uv(this._openDialogsAtThisLevel,e=>{e.config.closeOnDestroy===!1&&this._removeOpenDialog(e,!1)}),uv(this._openDialogsAtThisLevel,e=>e.close()),this._afterAllClosedAtThisLevel.complete(),this._afterOpenedAtThisLevel.complete(),this._openDialogsAtThisLevel=[]}_getOverlayConfig(e){let i=new In({positionStrategy:e.positionStrategy||Yn().centerHorizontally().centerVertically(),scrollStrategy:e.scrollStrategy||this._scrollStrategy(),panelClass:e.panelClass,hasBackdrop:e.hasBackdrop,direction:e.direction,minWidth:e.minWidth,minHeight:e.minHeight,maxWidth:e.maxWidth,maxHeight:e.maxHeight,width:e.width,height:e.height,disposeOnNavigation:e.closeOnNavigation,disableAnimations:e.disableAnimations});return e.backdropClass&&(i.backdropClass=e.backdropClass),i}_attachContainer(e,i,r){let o=r.injector||r.viewContainerRef?.injector,s=[{provide:sr,useValue:r},{provide:vo,useValue:i},{provide:Is,useValue:e}],a;r.container?typeof r.container=="function"?a=r.container:(a=r.container.type,s.push(...r.container.providers(r))):a=Nl;let l=new En(a,r.viewContainerRef,R.create({parent:o||this._injector,providers:s}));return e.attach(l).instance}_attachDialogContent(e,i,r,o){if(e instanceof Ct){let s=this._createInjector(o,i,r,void 0),a={$implicit:o.data,dialogRef:i};o.templateContext&&(a=v(v({},a),typeof o.templateContext=="function"?o.templateContext():o.templateContext)),r.attachTemplatePortal(new xn(e,null,a,s))}else{let s=this._createInjector(o,i,r,this._injector),a=r.attachComponentPortal(new En(e,o.viewContainerRef,s,null,o.bindings));i.componentRef=a,i.componentInstance=a.instance}}_createInjector(e,i,r,o){let s=e.injector||e.viewContainerRef?.injector,a=[{provide:MP,useValue:e.data},{provide:vo,useValue:i}];return e.providers&&(typeof e.providers=="function"?a.push(...e.providers(i,e,r)):a.push(...e.providers)),e.direction&&(!s||!s.get(ct,null,{optional:!0}))&&a.push({provide:ct,useValue:NP(e.direction)}),R.create({parent:s||o,providers:a})}_removeOpenDialog(e,i){let r=this.openDialogs.indexOf(e);r>-1&&(this.openDialogs.splice(r,1),this.openDialogs.length||(this._ariaHiddenElements.forEach((o,s)=>{o?s.setAttribute("aria-hidden",o):s.removeAttribute("aria-hidden")}),this._ariaHiddenElements.clear(),i&&this._getAfterAllClosed().next()))}_hideNonDialogContentFromAssistiveTechnology(e){if(e.parentElement){let i=e.parentElement.children;for(let r=i.length-1;r>-1;r--){let o=i[r];o!==e&&o.nodeName!=="SCRIPT"&&o.nodeName!=="STYLE"&&!o.hasAttribute("aria-live")&&!o.hasAttribute("popover")&&(this._ariaHiddenElements.set(o,o.getAttribute("aria-hidden")),o.setAttribute("aria-hidden","true"))}}}_getAfterAllClosed(){let e=this._parentDialog;return e?e._getAfterAllClosed():this._afterAllClosedAtThisLevel}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();function uv(t,n){let e=t.length;for(;e--;)n(t[e])}var fv=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({providers:[kl],imports:[an,_i,Il,_i]})}return t})();var Ei={XSmall:"(max-width: 599.98px)",Small:"(min-width: 600px) and (max-width: 959.98px)",Medium:"(min-width: 960px) and (max-width: 1279.98px)",Large:"(min-width: 1280px) and (max-width: 1919.98px)",XLarge:"(min-width: 1920px)",Handset:"(max-width: 599.98px) and (orientation: portrait), (max-width: 959.98px) and (orientation: landscape)",Tablet:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait), (min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",Web:"(min-width: 840px) and (orientation: portrait), (min-width: 1280px) and (orientation: landscape)",HandsetPortrait:"(max-width: 599.98px) and (orientation: portrait)",TabletPortrait:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait)",WebPortrait:"(min-width: 840px) and (orientation: portrait)",HandsetLandscape:"(max-width: 959.98px) and (orientation: landscape)",TabletLandscape:"(min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",WebLandscape:"(min-width: 1280px) and (orientation: landscape)"};var kP=new b("MATERIAL_ANIMATIONS"),ZD=null;function AP(){return u(kP,{optional:!0})?.animationsDisabled||u(Ea,{optional:!0})==="NoopAnimations"?"di-disabled":(ZD??=u(xs).matchMedia("(prefers-reduced-motion)").matches,ZD?"reduced-motion":"enabled")}function Ee(){return AP()!=="enabled"}function RP(t,n){}var XD="_mat-bottom-sheet-enter",KD="_mat-bottom-sheet-exit",OP=(()=>{class t extends Nl{_breakpointSubscription;_animationsDisabled=Ee();_animationState="void";_animationStateChanged=new W;_destroyed=!1;constructor(){super();let e=u(ho);this._breakpointSubscription=e.observe([Ei.Medium,Ei.Large,Ei.XLarge]).subscribe(()=>{let i=this._elementRef.nativeElement.classList;i.toggle("mat-bottom-sheet-container-medium",e.isMatched(Ei.Medium)),i.toggle("mat-bottom-sheet-container-large",e.isMatched(Ei.Large)),i.toggle("mat-bottom-sheet-container-xlarge",e.isMatched(Ei.XLarge))})}enter(){this._destroyed||(this._animationState="visible",this._changeDetectorRef.markForCheck(),this._changeDetectorRef.detectChanges(),this._animationsDisabled&&this._simulateAnimation(XD))}exit(){this._destroyed||(this._elementRef.nativeElement.setAttribute("mat-exit",""),this._animationState="hidden",this._changeDetectorRef.markForCheck(),this._animationsDisabled&&this._simulateAnimation(KD))}ngOnDestroy(){super.ngOnDestroy(),this._breakpointSubscription.unsubscribe(),this._destroyed=!0}_simulateAnimation(e){this._ngZone.run(()=>{this._handleAnimationEvent(!0,e,this._elementRef.nativeElement),setTimeout(()=>this._handleAnimationEvent(!1,e,this._elementRef.nativeElement))})}_trapFocus(){super._trapFocus({preventScroll:!0})}_handleAnimationEvent(e,i,r){if(r===this._elementRef.nativeElement){let o=i===XD;(o||i===KD)&&this._animationStateChanged.emit({toState:o?"visible":"hidden",phase:e?"start":"done"})}}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-bottom-sheet-container"]],hostAttrs:["tabindex","-1",1,"mat-bottom-sheet-container"],hostVars:9,hostBindings:function(i,r){i&1&&D("animationstart",function(s){return r._handleAnimationEvent(!0,s.animationName,s.target)})("animationend",function(s){return r._handleAnimationEvent(!1,s.animationName,s.target)})("animationcancel",function(s){return r._handleAnimationEvent(!1,s.animationName,s.target)}),i&2&&(re("role",r._config.role)("aria-modal",r._config.ariaModal)("aria-label",r._config.ariaLabel),B("mat-bottom-sheet-container-animations-enabled",!r._animationsDisabled)("mat-bottom-sheet-container-enter",r._animationState==="visible")("mat-bottom-sheet-container-exit",r._animationState==="hidden"))},features:[Te],decls:1,vars:0,consts:[["cdkPortalOutlet",""]],template:function(i,r){i&1&&Dt(0,RP,0,0,"ng-template",0)},dependencies:[qn],styles:[`@keyframes _mat-bottom-sheet-enter {
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
`],encapsulation:2,changeDetection:1})}return t})(),mv=new b("MatBottomSheetData"),hv=class{viewContainerRef;injector;panelClass;direction;data=null;hasBackdrop=!0;backdropClass;disableClose=!1;ariaLabel=null;ariaModal=!1;closeOnNavigation=!0;autoFocus="first-tabbable";restoreFocus=!0;scrollStrategy;height="";minHeight;maxHeight;bindings},Ts=class{_ref;get instance(){return this._ref.componentInstance}get componentRef(){return this._ref.componentRef}containerInstance;disableClose;_afterOpened=new x;_result;_closeFallbackTimeout;constructor(n,e,i){this._ref=n,this.containerInstance=i,this.disableClose=e.disableClose,i._animationStateChanged.pipe(ge(r=>r.phase==="done"&&r.toState==="visible"),Ae(1)).subscribe(()=>{this._afterOpened.next(),this._afterOpened.complete()}),i._animationStateChanged.pipe(ge(r=>r.phase==="done"&&r.toState==="hidden"),Ae(1)).subscribe(()=>{clearTimeout(this._closeFallbackTimeout),this._ref.close(this._result)}),n.overlayRef.detachments().subscribe(()=>{this._ref.close(this._result)}),Nt(this.backdropClick(),this.keydownEvents().pipe(ge(r=>r.keyCode===27))).subscribe(r=>{!this.disableClose&&(r.type!=="keydown"||!$e(r))&&(r.preventDefault(),this.dismiss())})}dismiss(n){this.containerInstance&&(this.containerInstance._animationStateChanged.pipe(ge(e=>e.phase==="start"),Ae(1)).subscribe(()=>{this._closeFallbackTimeout=setTimeout(()=>this._ref.close(this._result),500),this._ref.overlayRef.detachBackdrop()}),this._result=n,this.containerInstance.exit(),this.containerInstance=null)}afterDismissed(){return this._ref.closed}afterOpened(){return this._afterOpened}backdropClick(){return this._ref.backdropClick}keydownEvents(){return this._ref.keydownEvents}},PP=new b("mat-bottom-sheet-default-options"),QD=(()=>{class t{_injector=u(R);_parentBottomSheet=u(t,{optional:!0,skipSelf:!0});_animationsDisabled=Ee();_defaultOptions=u(PP,{optional:!0});_bottomSheetRefAtThisLevel=null;_dialog=u(kl);get _openedBottomSheetRef(){let e=this._parentBottomSheet;return e?e._openedBottomSheetRef:this._bottomSheetRefAtThisLevel}set _openedBottomSheetRef(e){this._parentBottomSheet?this._parentBottomSheet._openedBottomSheetRef=e:this._bottomSheetRefAtThisLevel=e}open(e,i){let r=v(v({},this._defaultOptions||new hv),i),o;return this._dialog.open(e,N(v({},r),{disableClose:!0,closeOnOverlayDetachments:!1,maxWidth:"100%",container:OP,scrollStrategy:r.scrollStrategy||or(this._injector),positionStrategy:Yn(this._injector).centerHorizontally().bottom("0"),disableAnimations:this._animationsDisabled,templateContext:()=>({bottomSheetRef:o}),providers:(s,a,l)=>(o=new Ts(s,r,l),[{provide:Ts,useValue:o},{provide:mv,useValue:r.data}])})),o.afterDismissed().subscribe(()=>{this._openedBottomSheetRef===o&&(this._openedBottomSheetRef=null)}),this._openedBottomSheetRef?(this._openedBottomSheetRef.afterDismissed().subscribe(()=>o.containerInstance?.enter()),this._openedBottomSheetRef.dismiss()):o.containerInstance.enter(),this._openedBottomSheetRef=o,o}dismiss(e){this._openedBottomSheetRef&&this._openedBottomSheetRef.dismiss(e)}ngOnDestroy(){this._bottomSheetRefAtThisLevel&&this._bottomSheetRefAtThisLevel.dismiss()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();var FP=200,of=class{_letterKeyStream=new x;_items=[];_selectedItemIndex=-1;_pressedLetters=[];_skipPredicateFn;_selectedItem=new x;selectedItem=this._selectedItem;constructor(n,e){let i=typeof e?.debounceInterval=="number"?e.debounceInterval:FP;e?.skipPredicate&&(this._skipPredicateFn=e.skipPredicate),this.setItems(n),this._setupKeyHandler(i)}destroy(){this._pressedLetters=[],this._letterKeyStream.complete(),this._selectedItem.complete()}setCurrentSelectedItemIndex(n){this._selectedItemIndex=n}setItems(n){this._items=n}handleKey(n){let e=n.keyCode;n.key&&n.key.length===1?this._letterKeyStream.next(n.key.toLocaleUpperCase()):(e>=65&&e<=90||e>=48&&e<=57)&&this._letterKeyStream.next(String.fromCharCode(e))}isTyping(){return this._pressedLetters.length>0}reset(){this._pressedLetters=[]}_setupKeyHandler(n){this._letterKeyStream.pipe(dt(e=>this._pressedLetters.push(e)),oa(n),ge(()=>this._pressedLetters.length>0),oe(()=>this._pressedLetters.join("").toLocaleUpperCase())).subscribe(e=>{for(let i=1;i<this._items.length+1;i++){let r=(this._selectedItemIndex+i)%this._items.length,o=this._items[r];if(!this._skipPredicateFn?.(o)&&o.getLabel?.().toLocaleUpperCase().trim().indexOf(e)===0){this._selectedItem.next(o);break}}this._pressedLetters=[]})}};var Ns=class{_items;_activeItemIndex=U(-1);_activeItem=U(null);_wrap=!1;_typeaheadSubscription=pe.EMPTY;_itemChangesSubscription;_vertical=!0;_horizontal=null;_allowedModifierKeys=[];_homeAndEnd=!1;_pageUpAndDown={enabled:!1,delta:10};_effectRef;_typeahead;_skipPredicateFn=n=>n.disabled;constructor(n,e){this._items=n,n instanceof ai?this._itemChangesSubscription=n.changes.subscribe(i=>this._itemsChanged(i.toArray())):Kt(n)&&(this._effectRef=ut(()=>this._itemsChanged(n()),{injector:e}))}tabOut=new x;change=new x;skipPredicate(n){return this._skipPredicateFn=n,this}withWrap(n=!0){return this._wrap=n,this}withVerticalOrientation(n=!0){return this._vertical=n,this}withHorizontalOrientation(n){return this._horizontal=n,this}withAllowedModifierKeys(n){return this._allowedModifierKeys=n,this}withTypeAhead(n=200){this._typeaheadSubscription.unsubscribe();let e=this._getItemsArray();return this._typeahead=new of(e,{debounceInterval:typeof n=="number"?n:void 0,skipPredicate:i=>this._skipPredicateFn(i)}),this._typeaheadSubscription=this._typeahead.selectedItem.subscribe(i=>{this.setActiveItem(i)}),this}cancelTypeahead(){return this._typeahead?.reset(),this}withHomeAndEnd(n=!0){return this._homeAndEnd=n,this}withPageUpDown(n=!0,e=10){return this._pageUpAndDown={enabled:n,delta:e},this}setActiveItem(n){let e=this._activeItem();this.updateActiveItem(n),this._activeItem()!==e&&this.change.next(this._activeItemIndex())}onKeydown(n){let e=n.keyCode,r=["altKey","ctrlKey","metaKey","shiftKey"].every(o=>!n[o]||this._allowedModifierKeys.indexOf(o)>-1);switch(e){case 9:this.tabOut.next();return;case 40:if(this._vertical&&r){this.setNextItemActive();break}else return;case 38:if(this._vertical&&r){this.setPreviousItemActive();break}else return;case 39:if(this._horizontal&&r){this._horizontal==="rtl"?this.setPreviousItemActive():this.setNextItemActive();break}else return;case 37:if(this._horizontal&&r){this._horizontal==="rtl"?this.setNextItemActive():this.setPreviousItemActive();break}else return;case 36:if(this._homeAndEnd&&r){this.setFirstItemActive();break}else return;case 35:if(this._homeAndEnd&&r){this.setLastItemActive();break}else return;case 33:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()-this._pageUpAndDown.delta;this._setActiveItemByIndex(o>0?o:0,1);break}else return;case 34:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()+this._pageUpAndDown.delta,s=this._getItemsArray().length;this._setActiveItemByIndex(o<s?o:s-1,-1);break}else return;default:(r||$e(n,"shiftKey"))&&this._typeahead?.handleKey(n);return}this._typeahead?.reset(),n.preventDefault()}get activeItemIndex(){return this._activeItemIndex()}get activeItem(){return this._activeItem()}isTyping(){return!!this._typeahead&&this._typeahead.isTyping()}setFirstItemActive(){this._setActiveItemByIndex(0,1)}setLastItemActive(){this._setActiveItemByIndex(this._getItemsArray().length-1,-1)}setNextItemActive(){this._activeItemIndex()<0?this.setFirstItemActive():this._setActiveItemByDelta(1)}setPreviousItemActive(){this._activeItemIndex()<0&&this._wrap?this.setLastItemActive():this._setActiveItemByDelta(-1)}updateActiveItem(n){let e=this._getItemsArray(),i=typeof n=="number"?n:e.indexOf(n),r=e[i];this._activeItem.set(r??null),this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i)}destroy(){this._typeaheadSubscription.unsubscribe(),this._itemChangesSubscription?.unsubscribe(),this._effectRef?.destroy(),this._typeahead?.destroy(),this.tabOut.complete(),this.change.complete()}_setActiveItemByDelta(n){this._wrap?this._setActiveInWrapMode(n):this._setActiveInDefaultMode(n)}_setActiveInWrapMode(n){let e=this._getItemsArray();for(let i=1;i<=e.length;i++){let r=(this._activeItemIndex()+n*i+e.length)%e.length,o=e[r];if(!this._skipPredicateFn(o)){this.setActiveItem(r);return}}}_setActiveInDefaultMode(n){this._setActiveItemByIndex(this._activeItemIndex()+n,n)}_setActiveItemByIndex(n,e){let i=this._getItemsArray();if(i[n]){for(;this._skipPredicateFn(i[n]);)if(n+=e,!i[n])return;this.setActiveItem(n)}}_getItemsArray(){return Kt(this._items)?this._items():this._items instanceof ai?this._items.toArray():this._items}_itemsChanged(n){this._typeahead?.setItems(n);let e=this._activeItem();if(e){let i=n.indexOf(e);i>-1&&i!==this._activeItemIndex()&&(this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i))}}};var Al=class extends Ns{setActiveItem(n){this.activeItem&&this.activeItem.setInactiveStyles(),super.setActiveItem(n),this.activeItem&&this.activeItem.setActiveStyles()}};var Rl=class extends Ns{_origin="program";setFocusOrigin(n){return this._origin=n,this}setActiveItem(n){super.setActiveItem(n),this.activeItem&&this.activeItem.focus(this._origin)}};var tE=" ";function LP(t,n,e){let i=af(t,n);e=e.trim(),!i.some(r=>r.trim()===e)&&(i.push(e),t.setAttribute(n,i.join(tE)))}function VP(t,n,e){let i=af(t,n);e=e.trim();let r=i.filter(o=>o!==e);r.length?t.setAttribute(n,r.join(tE)):t.removeAttribute(n)}function af(t,n){return t.getAttribute(n)?.match(/\S+/g)??[]}var nE="cdk-describedby-message",sf="cdk-describedby-host",gv=0,iE=(()=>{class t{_platform=u(be);_document=u(j);_messageRegistry=new Map;_messagesContainer=null;_id=`${gv++}`;constructor(){u(ze).load(Ds),this._id=u(si)+"-"+gv++}describe(e,i,r){if(!this._canBeDescribed(e,i))return;let o=pv(i,r);typeof i!="string"?(eE(i,this._id),this._messageRegistry.set(o,{messageElement:i,referenceCount:0})):this._messageRegistry.has(o)||this._createMessageElement(i,r),this._isElementDescribedByMessage(e,o)||this._addMessageReference(e,o)}removeDescription(e,i,r){if(!i||!this._isElementNode(e))return;let o=pv(i,r);if(this._isElementDescribedByMessage(e,o)&&this._removeMessageReference(e,o),typeof i=="string"){let s=this._messageRegistry.get(o);s&&s.referenceCount===0&&this._deleteMessageElement(o)}this._messagesContainer?.childNodes.length===0&&(this._messagesContainer.remove(),this._messagesContainer=null)}ngOnDestroy(){let e=this._document.querySelectorAll(`[${sf}="${this._id}"]`);for(let i=0;i<e.length;i++)this._removeCdkDescribedByReferenceIds(e[i]),e[i].removeAttribute(sf);this._messagesContainer?.remove(),this._messagesContainer=null,this._messageRegistry.clear()}_createMessageElement(e,i){let r=this._document.createElement("div");eE(r,this._id),r.textContent=e,i&&r.setAttribute("role",i),this._createMessagesContainer(),this._messagesContainer.appendChild(r),this._messageRegistry.set(pv(e,i),{messageElement:r,referenceCount:0})}_deleteMessageElement(e){this._messageRegistry.get(e)?.messageElement?.remove(),this._messageRegistry.delete(e)}_createMessagesContainer(){if(this._messagesContainer)return;let e="cdk-describedby-message-container",i=this._document.querySelectorAll(`.${e}[platform="server"]`);for(let o=0;o<i.length;o++)i[o].remove();let r=this._document.createElement("div");r.style.visibility="hidden",r.classList.add(e),r.classList.add("cdk-visually-hidden"),this._platform.isBrowser||r.setAttribute("platform","server"),this._document.body.appendChild(r),this._messagesContainer=r}_removeCdkDescribedByReferenceIds(e){let i=af(e,"aria-describedby").filter(r=>r.indexOf(nE)!=0);e.setAttribute("aria-describedby",i.join(" "))}_addMessageReference(e,i){let r=this._messageRegistry.get(i);LP(e,"aria-describedby",r.messageElement.id),e.setAttribute(sf,this._id),r.referenceCount++}_removeMessageReference(e,i){let r=this._messageRegistry.get(i);r.referenceCount--,VP(e,"aria-describedby",r.messageElement.id),e.removeAttribute(sf)}_isElementDescribedByMessage(e,i){let r=af(e,"aria-describedby"),o=this._messageRegistry.get(i),s=o&&o.messageElement.id;return!!s&&r.indexOf(s)!=-1}_canBeDescribed(e,i){if(!this._isElementNode(e))return!1;if(i&&typeof i=="object")return!0;let r=i==null?"":`${i}`.trim(),o=e.getAttribute("aria-label");return r?!o||o.trim()!==r:!1}_isElementNode(e){return e.nodeType===this._document.ELEMENT_NODE}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();function pv(t,n){return typeof t=="string"?`${n||""}/${t}`:t}function eE(t,n){t.id||(t.id=`${nE}-${n}-${gv++}`)}var ks,rE=["color","button","checkbox","date","datetime-local","email","file","hidden","image","month","number","password","radio","range","reset","search","submit","tel","text","time","url","week"];function vv(){if(ks)return ks;if(typeof document!="object"||!document)return ks=new Set(rE),ks;let t=document.createElement("input");return ks=new Set(rE.filter(n=>(t.setAttribute("type",n),t.type===n))),ks}function ln(t){return t!=null&&`${t}`!="false"}var cn=(function(t){return t[t.FADING_IN=0]="FADING_IN",t[t.VISIBLE=1]="VISIBLE",t[t.FADING_OUT=2]="FADING_OUT",t[t.HIDDEN=3]="HIDDEN",t})(cn||{}),bv=class{_renderer;element;config;_animationForciblyDisabledThroughCss;state=cn.HIDDEN;constructor(n,e,i,r=!1){this._renderer=n,this.element=e,this.config=i,this._animationForciblyDisabledThroughCss=r}fadeOut(){this._renderer.fadeOutRipple(this)}},oE=Cs({passive:!0,capture:!0}),_v=class{_events=new Map;addHandler(n,e,i,r){let o=this._events.get(e);if(o){let s=o.get(i);s?s.add(r):o.set(i,new Set([r]))}else this._events.set(e,new Map([[i,new Set([r])]])),n.runOutsideAngular(()=>{document.addEventListener(e,this._delegateEventHandler,oE)})}removeHandler(n,e,i){let r=this._events.get(n);if(!r)return;let o=r.get(e);o&&(o.delete(i),o.size===0&&r.delete(e),r.size===0&&(this._events.delete(n),document.removeEventListener(n,this._delegateEventHandler,oE)))}_delegateEventHandler=n=>{let e=yt(n);e&&this._events.get(n.type)?.forEach((i,r)=>{(r===e||r.contains(e))&&i.forEach(o=>o.handleEvent(n))})}},Ol={enterDuration:225,exitDuration:150},BP=800,sE=Cs({passive:!0,capture:!0}),aE=["mousedown","touchstart"],lE=["mouseup","mouseleave","touchend","touchcancel"],jP=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["ng-component"]],hostAttrs:["mat-ripple-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.mat-ripple {
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
`],encapsulation:2})}return t})(),Pl=class t{_target;_ngZone;_platform;_containerElement;_triggerElement=null;_isPointerDown=!1;_activeRipples=new Map;_mostRecentTransientRipple=null;_lastTouchStartEvent;_pointerUpEventsRegistered=!1;_containerRect=null;static _eventManager=new _v;constructor(n,e,i,r,o){this._target=n,this._ngZone=e,this._platform=r,r.isBrowser&&(this._containerElement=Wt(i)),o&&o.get(ze).load(jP)}fadeInRipple(n,e,i={}){let r=this._containerRect=this._containerRect||this._containerElement.getBoundingClientRect(),o=v(v({},Ol),i.animation);i.centered&&(n=r.left+r.width/2,e=r.top+r.height/2);let s=i.radius||HP(n,e,r),a=n-r.left,l=e-r.top,c=o.enterDuration,d=document.createElement("div");d.classList.add("mat-ripple-element"),d.style.left=`${a-s}px`,d.style.top=`${l-s}px`,d.style.height=`${s*2}px`,d.style.width=`${s*2}px`,i.color!=null&&(d.style.backgroundColor=i.color),d.style.transitionDuration=`${c}ms`,this._containerElement.appendChild(d);let f=window.getComputedStyle(d),h=f.transitionProperty,p=f.transitionDuration,S=h==="none"||p==="0s"||p==="0s, 0s"||r.width===0&&r.height===0,E=new bv(this,d,i,S);d.style.transform="scale3d(1, 1, 1)",E.state=cn.FADING_IN,i.persistent||(this._mostRecentTransientRipple=E);let $=null;return!S&&(c||o.exitDuration)&&this._ngZone.runOutsideAngular(()=>{let H=()=>{$&&($.fallbackTimer=null),clearTimeout(Ge),this._finishRippleTransition(E)},Se=()=>this._destroyRipple(E),Ge=setTimeout(Se,c+100);d.addEventListener("transitionend",H),d.addEventListener("transitioncancel",Se),$={onTransitionEnd:H,onTransitionCancel:Se,fallbackTimer:Ge}}),this._activeRipples.set(E,$),(S||!c)&&this._finishRippleTransition(E),E}fadeOutRipple(n){if(n.state===cn.FADING_OUT||n.state===cn.HIDDEN)return;let e=n.element,i=v(v({},Ol),n.config.animation);e.style.transitionDuration=`${i.exitDuration}ms`,e.style.opacity="0",n.state=cn.FADING_OUT,(n._animationForciblyDisabledThroughCss||!i.exitDuration)&&this._finishRippleTransition(n)}fadeOutAll(){this._getActiveRipples().forEach(n=>n.fadeOut())}fadeOutAllNonPersistent(){this._getActiveRipples().forEach(n=>{n.config.persistent||n.fadeOut()})}setupTriggerEvents(n){let e=Wt(n);!this._platform.isBrowser||!e||e===this._triggerElement||(this._removeTriggerEvents(),this._triggerElement=e,aE.forEach(i=>{t._eventManager.addHandler(this._ngZone,i,e,this)}))}handleEvent(n){n.type==="mousedown"?this._onMousedown(n):n.type==="touchstart"?this._onTouchStart(n):this._onPointerUp(),this._pointerUpEventsRegistered||(this._ngZone.runOutsideAngular(()=>{lE.forEach(e=>{this._triggerElement.addEventListener(e,this,sE)})}),this._pointerUpEventsRegistered=!0)}_finishRippleTransition(n){n.state===cn.FADING_IN?this._startFadeOutTransition(n):n.state===cn.FADING_OUT&&this._destroyRipple(n)}_startFadeOutTransition(n){let e=n===this._mostRecentTransientRipple,{persistent:i}=n.config;n.state=cn.VISIBLE,!i&&(!e||!this._isPointerDown)&&n.fadeOut()}_destroyRipple(n){let e=this._activeRipples.get(n)??null;this._activeRipples.delete(n),this._activeRipples.size||(this._containerRect=null),n===this._mostRecentTransientRipple&&(this._mostRecentTransientRipple=null),n.state=cn.HIDDEN,e!==null&&(n.element.removeEventListener("transitionend",e.onTransitionEnd),n.element.removeEventListener("transitioncancel",e.onTransitionCancel),e.fallbackTimer!==null&&clearTimeout(e.fallbackTimer)),n.element.remove()}_onMousedown(n){let e=lo(n),i=this._lastTouchStartEvent&&Date.now()<this._lastTouchStartEvent+BP;!this._target.rippleDisabled&&!e&&!i&&(this._isPointerDown=!0,this.fadeInRipple(n.clientX,n.clientY,this._target.rippleConfig))}_onTouchStart(n){if(!this._target.rippleDisabled&&!co(n)){this._lastTouchStartEvent=Date.now(),this._isPointerDown=!0;let e=n.changedTouches;if(e)for(let i=0;i<e.length;i++)this.fadeInRipple(e[i].clientX,e[i].clientY,this._target.rippleConfig)}}_onPointerUp(){this._isPointerDown&&(this._isPointerDown=!1,this._getActiveRipples().forEach(n=>{let e=n.state===cn.VISIBLE||n.config.terminateOnPointerUp&&n.state===cn.FADING_IN;!n.config.persistent&&e&&n.fadeOut()}))}_getActiveRipples(){return Array.from(this._activeRipples.keys())}_removeTriggerEvents(){let n=this._triggerElement;n&&(aE.forEach(e=>t._eventManager.removeHandler(e,n,this)),this._pointerUpEventsRegistered&&(lE.forEach(e=>n.removeEventListener(e,this,sE)),this._pointerUpEventsRegistered=!1))}};function HP(t,n,e){let i=Math.max(Math.abs(t-e.left),Math.abs(t-e.right)),r=Math.max(Math.abs(n-e.top),Math.abs(n-e.bottom));return Math.sqrt(i*i+r*r)}var yv=new b("mat-ripple-global-options"),ar=(()=>{class t{_elementRef=u(O);_animationsDisabled=Ee();color;unbounded=!1;centered=!1;radius=0;animation;get disabled(){return this._disabled}set disabled(e){e&&this.fadeOutAllNonPersistent(),this._disabled=e,this._setupTriggerEventsIfEnabled()}_disabled=!1;get trigger(){return this._trigger||this._elementRef.nativeElement}set trigger(e){this._trigger=e,this._setupTriggerEventsIfEnabled()}_trigger;_rippleRenderer;_globalOptions;_isInitialized=!1;constructor(){let e=u(k),i=u(be),r=u(yv,{optional:!0}),o=u(R);this._globalOptions=r||{},this._rippleRenderer=new Pl(this,e,this._elementRef,i,o)}ngOnInit(){this._isInitialized=!0,this._setupTriggerEventsIfEnabled()}ngOnDestroy(){this._rippleRenderer._removeTriggerEvents()}fadeOutAll(){this._rippleRenderer.fadeOutAll()}fadeOutAllNonPersistent(){this._rippleRenderer.fadeOutAllNonPersistent()}get rippleConfig(){return{centered:this.centered,radius:this.radius,color:this.color,animation:v(v(v({},this._globalOptions.animation),this._animationsDisabled?{enterDuration:0,exitDuration:0}:{}),this.animation),terminateOnPointerUp:this._globalOptions.terminateOnPointerUp}}get rippleDisabled(){return this.disabled||!!this._globalOptions.disabled}_setupTriggerEventsIfEnabled(){!this.disabled&&this._isInitialized&&this._rippleRenderer.setupTriggerEvents(this.trigger)}launch(e,i=0,r){return typeof e=="number"?this._rippleRenderer.fadeInRipple(e,i,v(v({},this.rippleConfig),r)):this._rippleRenderer.fadeInRipple(0,0,v(v({},this.rippleConfig),e))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=z({type:t,selectors:[["","mat-ripple",""],["","matRipple",""]],hostAttrs:[1,"mat-ripple"],hostVars:2,hostBindings:function(i,r){i&2&&B("mat-ripple-unbounded",r.unbounded)},inputs:{color:[0,"matRippleColor","color"],unbounded:[0,"matRippleUnbounded","unbounded"],centered:[0,"matRippleCentered","centered"],radius:[0,"matRippleRadius","radius"],animation:[0,"matRippleAnimation","animation"],disabled:[0,"matRippleDisabled","disabled"],trigger:[0,"matRippleTrigger","trigger"]},exportAs:["matRipple"]})}return t})();var UP={capture:!0},zP=["focus","mousedown","mouseenter","touchstart"],Sv="mat-ripple-loader-uninitialized",Cv="mat-ripple-loader-class-name",cE="mat-ripple-loader-centered",lf="mat-ripple-loader-disabled",dE=(()=>{class t{_document=u(j);_animationsDisabled=Ee();_globalRippleOptions=u(yv,{optional:!0});_platform=u(be);_ngZone=u(k);_injector=u(R);_eventCleanups;_hosts=new Map;constructor(){let e=u(Qe).createRenderer(null,null);this._eventCleanups=this._ngZone.runOutsideAngular(()=>zP.map(i=>e.listen(this._document,i,this._onInteraction,UP)))}ngOnDestroy(){let e=this._hosts.keys();for(let i of e)this.destroyRipple(i);this._eventCleanups.forEach(i=>i())}configureRipple(e,i){e.setAttribute(Sv,this._globalRippleOptions?.namespace??""),(i.className||!e.hasAttribute(Cv))&&e.setAttribute(Cv,i.className||""),i.centered&&e.setAttribute(cE,""),i.disabled&&e.setAttribute(lf,"")}setDisabled(e,i){let r=this._hosts.get(e);r?(r.target.rippleDisabled=i,!i&&!r.hasSetUpEvents&&(r.hasSetUpEvents=!0,r.renderer.setupTriggerEvents(e))):i?e.setAttribute(lf,""):e.removeAttribute(lf)}_onInteraction=e=>{let i=yt(e);if(i instanceof HTMLElement){let r=i.closest(`[${Sv}="${this._globalRippleOptions?.namespace??""}"]`);r&&this._createRipple(r)}};_createRipple(e){if(!this._document||this._hosts.has(e))return;e.querySelector(".mat-ripple")?.remove();let i=this._document.createElement("span");i.classList.add("mat-ripple",e.getAttribute(Cv)),e.append(i);let r=this._globalRippleOptions,o=this._animationsDisabled?0:r?.animation?.enterDuration??Ol.enterDuration,s=this._animationsDisabled?0:r?.animation?.exitDuration??Ol.exitDuration,a={rippleDisabled:this._animationsDisabled||r?.disabled||e.hasAttribute(lf),rippleConfig:{centered:e.hasAttribute(cE),terminateOnPointerUp:r?.terminateOnPointerUp,animation:{enterDuration:o,exitDuration:s}}},l=new Pl(a,this._ngZone,i,this._platform,this._injector),c=!a.rippleDisabled;c&&l.setupTriggerEvents(e),this._hosts.set(e,{target:a,renderer:l,hasSetUpEvents:c}),e.removeAttribute(Sv)}destroyRipple(e){let i=this._hosts.get(e);i&&(i.renderer._removeTriggerEvents(),this._hosts.delete(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();var Zn=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["structural-styles"]],decls:0,vars:0,template:function(i,r){},styles:[`.mat-focus-indicator {
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
`],encapsulation:2})}return t})();var $P=["*",[["","progressIndicator",""]]],GP=["*","[progressIndicator]"];function WP(t,n){t&1&&(lt(0,"div",1),me(1,1),_t())}var qP=new b("MAT_BUTTON_CONFIG");function uE(t){return t==null?void 0:Xr(t)}var Dv=(()=>{class t{_elementRef=u(O);_ngZone=u(k);_animationsDisabled=Ee();_config=u(qP,{optional:!0});_focusMonitor=u(sn);_cleanupClick;_renderer=u(Re);_rippleLoader=u(dE);_isAnchor;_isFab=!1;color;get disableRipple(){return this._disableRipple}set disableRipple(e){this._disableRipple=e,this._updateRippleDisabled()}_disableRipple=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._updateRippleDisabled()}_disabled=!1;ariaDisabled;disabledInteractive;tabIndex;set _tabindex(e){this.tabIndex=e}showProgress=zt(!1,{transform:V});constructor(){u(ze).load(Zn);let e=this._elementRef.nativeElement;this._isAnchor=e.tagName==="A",this.disabledInteractive=this._config?.disabledInteractive??!1,this.color=this._config?.color??null,this._rippleLoader?.configureRipple(e,{className:"mat-mdc-button-ripple"})}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0),this._isAnchor&&this._setupAsAnchor()}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement)}focus(e="program",i){e?this._focusMonitor.focusVia(this._elementRef.nativeElement,e,i):this._elementRef.nativeElement.focus(i)}_getAriaDisabled(){return this.ariaDisabled!=null?this.ariaDisabled:this._isAnchor?this.disabled||null:this.disabled&&this.disabledInteractive?!0:null}_getDisabledAttribute(){return this.disabledInteractive||!this.disabled?null:!0}_updateRippleDisabled(){this._rippleLoader?.setDisabled(this._elementRef.nativeElement,this.disableRipple||this.disabled)}_getTabIndex(){return this._isAnchor?this.disabled&&!this.disabledInteractive?-1:this.tabIndex:this.tabIndex}_setupAsAnchor(){this._cleanupClick=this._ngZone.runOutsideAngular(()=>this._renderer.listen(this._elementRef.nativeElement,"click",e=>{this.disabled&&(e.preventDefault(),e.stopImmediatePropagation())}))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=z({type:t,hostAttrs:[1,"mat-mdc-button-base"],hostVars:15,hostBindings:function(i,r){i&2&&(re("disabled",r._getDisabledAttribute())("aria-disabled",r._getAriaDisabled())("tabindex",r._getTabIndex()),xt(r.color?"mat-"+r.color:""),B("mat-mdc-button-progress-indicator-shown",r.showProgress())("mat-mdc-button-disabled",r.disabled)("mat-mdc-button-disabled-interactive",r.disabledInteractive)("mat-unthemed",!r.color)("_mat-animation-noopable",r._animationsDisabled))},inputs:{color:"color",disableRipple:[2,"disableRipple","disableRipple",V],disabled:[2,"disabled","disabled",V],ariaDisabled:[2,"aria-disabled","ariaDisabled",V],disabledInteractive:[2,"disabledInteractive","disabledInteractive",V],tabIndex:[2,"tabIndex","tabIndex",uE],_tabindex:[2,"tabindex","_tabindex",uE],showProgress:[1,"showProgress"]}})}return t})(),Co=(()=>{class t extends Dv{constructor(){super(),this._rippleLoader.configureRipple(this._elementRef.nativeElement,{centered:!0})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["button","mat-icon-button",""],["a","mat-icon-button",""],["button","matIconButton",""],["a","matIconButton",""]],hostAttrs:[1,"mdc-icon-button","mat-mdc-icon-button"],exportAs:["matButton","matAnchor"],features:[Te],ngContentSelectors:GP,decls:5,vars:1,consts:[[1,"mat-mdc-button-persistent-ripple","mdc-icon-button__ripple"],[1,"mat-mdc-button-progress-indicator-container"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(Pe($P),tn(0,"span",0),me(1),P(2,WP,2,0,"div",1),tn(3,"span",2)(4,"span",3)),i&2&&(_(2),F(r.showProgress()?2:-1))},styles:[`.mat-mdc-icon-button {
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
`],encapsulation:2})}return t})();var lr=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({imports:[ve]})}return t})();var YP=[[["",8,"material-icons",3,"iconPositionEnd",""],["mat-icon",3,"iconPositionEnd",""],["","matButtonIcon","",3,"iconPositionEnd",""]],"*",[["","iconPositionEnd","",8,"material-icons"],["mat-icon","iconPositionEnd",""],["","matButtonIcon","","iconPositionEnd",""]],[["","progressIndicator",""]]],ZP=[".material-icons:not([iconPositionEnd]), mat-icon:not([iconPositionEnd]), [matButtonIcon]:not([iconPositionEnd])","*",".material-icons[iconPositionEnd], mat-icon[iconPositionEnd], [matButtonIcon][iconPositionEnd]","[progressIndicator]"];function XP(t,n){t&1&&(lt(0,"div",2),me(1,3),_t())}var fE=new Map([["text",["mat-mdc-button"]],["filled",["mdc-button--unelevated","mat-mdc-unelevated-button"]],["elevated",["mdc-button--raised","mat-mdc-raised-button"]],["outlined",["mdc-button--outlined","mat-mdc-outlined-button"]],["tonal",["mat-tonal-button"]]]),cr=(()=>{class t extends Dv{get appearance(){return this._appearance}set appearance(e){this.setAppearance(e||this._config?.defaultAppearance||"text")}_appearance=null;constructor(){super();let e=KP(this._elementRef.nativeElement);e&&this.setAppearance(e)}setAppearance(e){if(e===this._appearance)return;let i=this._elementRef.nativeElement.classList,r=this._appearance?fE.get(this._appearance):null,o=fE.get(e);r&&i.remove(...r),i.add(...o),this._appearance=e}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["button","matButton",""],["a","matButton",""],["button","mat-button",""],["button","mat-raised-button",""],["button","mat-flat-button",""],["button","mat-stroked-button",""],["a","mat-button",""],["a","mat-raised-button",""],["a","mat-flat-button",""],["a","mat-stroked-button",""]],hostAttrs:[1,"mdc-button"],inputs:{appearance:[0,"matButton","appearance"]},exportAs:["matButton","matAnchor"],features:[Te],ngContentSelectors:ZP,decls:8,vars:5,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-mdc-button-progress-indicator-container"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(Pe(YP),tn(0,"span",0),me(1),lt(2,"span",1),me(3,1),_t(),me(4,2),P(5,XP,2,0,"div",2),tn(6,"span",3)(7,"span",4)),i&2&&(B("mdc-button__ripple",!r._isFab)("mdc-fab__ripple",r._isFab),_(5),F(r.showProgress()?5:-1))},styles:[`.mat-mdc-button-base {
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
`],encapsulation:2})}return t})();function KP(t){return t.hasAttribute("mat-raised-button")?"elevated":t.hasAttribute("mat-stroked-button")?"outlined":t.hasAttribute("mat-flat-button")?"filled":t.hasAttribute("mat-button")?"text":null}var Nn=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({imports:[lr,ve]})}return t})();var QP=["*"];var JP=new b("MAT_CARD_CONFIG"),hE=(()=>{class t{appearance;constructor(){let e=u(JP,{optional:!0});this.appearance=e?.appearance||"raised"}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-card"]],hostAttrs:[1,"mat-mdc-card","mdc-card"],hostVars:8,hostBindings:function(i,r){i&2&&B("mat-mdc-card-outlined",r.appearance==="outlined")("mdc-card--outlined",r.appearance==="outlined")("mat-mdc-card-filled",r.appearance==="filled")("mdc-card--filled",r.appearance==="filled")},inputs:{appearance:"appearance"},exportAs:["matCard"],ngContentSelectors:QP,decls:1,vars:0,template:function(i,r){i&1&&(Pe(),me(0))},styles:[`.mat-mdc-card {
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
`],encapsulation:2})}return t})();var mE=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=z({type:t,selectors:[["mat-card-content"]],hostAttrs:[1,"mat-mdc-card-content"]})}return t})();var pE=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({imports:[ve]})}return t})();function t1(t,n){}var df=class{viewContainerRef;injector;id;role="dialog";panelClass="";hasBackdrop=!0;backdropClass="";disableClose=!1;closePredicate;width="";height="";minWidth;minHeight;maxWidth;maxHeight;position;data=null;direction;ariaDescribedBy=null;ariaLabelledBy=null;ariaLabel=null;ariaModal=!1;autoFocus="first-tabbable";restoreFocus=!0;delayFocusTrap=!0;scrollStrategy;closeOnNavigation=!0;enterAnimationDuration;exitAnimationDuration;bindings},Ev="mdc-dialog--open",gE="mdc-dialog--opening",vE="mdc-dialog--closing",n1=150,i1=75,r1=(()=>{class t extends Nl{_animationStateChanged=new W;_animationsEnabled=!Ee();_actionSectionCount=0;_hostElement=this._elementRef.nativeElement;_enterAnimationDuration=this._animationsEnabled?_E(this._config.enterAnimationDuration)??n1:0;_exitAnimationDuration=this._animationsEnabled?_E(this._config.exitAnimationDuration)??i1:0;_animationTimer=null;_contentAttached(){super._contentAttached(),this._startOpenAnimation()}_startOpenAnimation(){this._animationStateChanged.emit({state:"opening",totalTime:this._enterAnimationDuration}),this._animationsEnabled?(this._hostElement.style.setProperty(bE,`${this._enterAnimationDuration}ms`),this._requestAnimationFrame(()=>this._hostElement.classList.add(gE,Ev)),this._waitForAnimationToComplete(this._enterAnimationDuration,this._finishDialogOpen)):(this._hostElement.classList.add(Ev),Promise.resolve().then(()=>this._finishDialogOpen()))}_startExitAnimation(){this._animationStateChanged.emit({state:"closing",totalTime:this._exitAnimationDuration}),this._hostElement.classList.remove(Ev),this._animationsEnabled?(this._hostElement.style.setProperty(bE,`${this._exitAnimationDuration}ms`),this._requestAnimationFrame(()=>this._hostElement.classList.add(vE)),this._waitForAnimationToComplete(this._exitAnimationDuration,this._finishDialogClose)):Promise.resolve().then(()=>this._finishDialogClose())}_updateActionSectionCount(e){this._actionSectionCount+=e,this._changeDetectorRef.markForCheck()}_finishDialogOpen=()=>{this._clearAnimationClasses(),this._openAnimationDone(this._enterAnimationDuration)};_finishDialogClose=()=>{this._clearAnimationClasses(),this._animationStateChanged.emit({state:"closed",totalTime:this._exitAnimationDuration})};_clearAnimationClasses(){this._hostElement.classList.remove(gE,vE)}_waitForAnimationToComplete(e,i){this._animationTimer!==null&&clearTimeout(this._animationTimer),this._animationTimer=setTimeout(i,e)}_requestAnimationFrame(e){this._ngZone.runOutsideAngular(()=>{typeof requestAnimationFrame=="function"?requestAnimationFrame(e):e()})}_captureInitialFocus(){this._config.delayFocusTrap||this._trapFocus()}_openAnimationDone(e){this._config.delayFocusTrap&&this._trapFocus(),this._animationStateChanged.next({state:"opened",totalTime:e})}ngOnDestroy(){super.ngOnDestroy(),this._animationTimer!==null&&clearTimeout(this._animationTimer)}attachComponentPortal(e){let i=super.attachComponentPortal(e);return i.location.nativeElement.classList.add("mat-mdc-dialog-component-host"),i}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ht(t)))(r||t)}})();static \u0275cmp=T({type:t,selectors:[["mat-dialog-container"]],hostAttrs:["tabindex","-1",1,"mat-mdc-dialog-container","mdc-dialog"],hostVars:10,hostBindings:function(i,r){i&2&&(Et("id",r._config.id),re("aria-modal",r._config.ariaModal)("role",r._config.role)("aria-labelledby",r._config.ariaLabel?null:r._ariaLabelledByQueue[0])("aria-label",r._config.ariaLabel)("aria-describedby",r._config.ariaDescribedBy||null),B("_mat-animation-noopable",!r._animationsEnabled)("mat-mdc-dialog-container-with-actions",r._actionSectionCount>0))},features:[Te],decls:3,vars:0,consts:[[1,"mat-mdc-dialog-inner-container","mdc-dialog__container"],[1,"mat-mdc-dialog-surface","mdc-dialog__surface"],["cdkPortalOutlet",""]],template:function(i,r){i&1&&(m(0,"div",0)(1,"div",1),Dt(2,t1,0,0,"ng-template",2),g()())},dependencies:[qn],styles:[`.mat-mdc-dialog-container {
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
`],encapsulation:2,changeDetection:1})}return t})(),bE="--mat-dialog-transition-duration";function _E(t){return t==null?null:typeof t=="number"?t:t.endsWith("ms")?yi(t.substring(0,t.length-2)):t.endsWith("s")?yi(t.substring(0,t.length-1))*1e3:t==="0"?0:null}var cf=(function(t){return t[t.OPEN=0]="OPEN",t[t.CLOSING=1]="CLOSING",t[t.CLOSED=2]="CLOSED",t})(cf||{}),As=class{_ref;_config;_containerInstance;componentInstance;componentRef=null;disableClose;id;_afterOpened=new ki(1);_beforeClosed=new ki(1);_result;_closeFallbackTimeout;_state=cf.OPEN;_closeInteractionType;constructor(n,e,i){this._ref=n,this._config=e,this._containerInstance=i,this.disableClose=e.disableClose,this.id=n.id,n.addPanelClass("mat-mdc-dialog-panel"),i._animationStateChanged.pipe(ge(r=>r.state==="opened"),Ae(1)).subscribe(()=>{this._afterOpened.next(),this._afterOpened.complete()}),i._animationStateChanged.pipe(ge(r=>r.state==="closed"),Ae(1)).subscribe(()=>{clearTimeout(this._closeFallbackTimeout),this._finishDialogClose()}),n.overlayRef.detachments().subscribe(()=>{this._beforeClosed.next(this._result),this._beforeClosed.complete(),this._finishDialogClose()}),Nt(this.backdropClick(),this.keydownEvents().pipe(ge(r=>r.keyCode===27&&!this.disableClose&&!$e(r)))).subscribe(r=>{this.disableClose||(r.preventDefault(),o1(this,r.type==="keydown"?"keyboard":"mouse"))})}close(n){let e=this._config.closePredicate;e&&!e(n,this._config,this.componentInstance)||(this._result=n,this._containerInstance._animationStateChanged.pipe(ge(i=>i.state==="closing"),Ae(1)).subscribe(i=>{this._beforeClosed.next(n),this._beforeClosed.complete(),this._ref.overlayRef.detachBackdrop(),this._closeFallbackTimeout=setTimeout(()=>this._finishDialogClose(),i.totalTime+100)}),this._state=cf.CLOSING,this._containerInstance._startExitAnimation())}afterOpened(){return this._afterOpened}afterClosed(){return this._ref.closed}beforeClosed(){return this._beforeClosed}backdropClick(){return this._ref.backdropClick}keydownEvents(){return this._ref.keydownEvents}updatePosition(n){let e=this._ref.config.positionStrategy;return n&&(n.left||n.right)?n.left?e.left(n.left):e.right(n.right):e.centerHorizontally(),n&&(n.top||n.bottom)?n.top?e.top(n.top):e.bottom(n.bottom):e.centerVertically(),this._ref.updatePosition(),this}updateSize(n="",e=""){return this._ref.updateSize(n,e),this}addPanelClass(n){return this._ref.addPanelClass(n),this}removePanelClass(n){return this._ref.removePanelClass(n),this}getState(){return this._state}_finishDialogClose(){this._state=cf.CLOSED,this._ref.close(this._result,{focusOrigin:this._closeInteractionType}),this.componentInstance=null}};function o1(t,n,e){return t._closeInteractionType=n,t.close(e)}var s1=new b("MatMdcDialogData"),a1=new b("mat-mdc-dialog-default-options"),l1=new b("mat-mdc-dialog-scroll-strategy",{providedIn:"root",factory:()=>{let t=u(R);return()=>or(t)}}),Rs=(()=>{class t{_defaultOptions=u(a1,{optional:!0});_scrollStrategy=u(l1);_parentDialog=u(t,{optional:!0,skipSelf:!0});_idGenerator=u(Ie);_injector=u(R);_dialog=u(kl);_animationsDisabled=Ee();_openDialogsAtThisLevel=[];_afterAllClosedAtThisLevel=new x;_afterOpenedAtThisLevel=new x;dialogConfigClass=df;_dialogRefConstructor;_dialogContainerType;_dialogDataToken;get openDialogs(){return this._parentDialog?this._parentDialog.openDialogs:this._openDialogsAtThisLevel}get afterOpened(){return this._parentDialog?this._parentDialog.afterOpened:this._afterOpenedAtThisLevel}_getAfterAllClosed(){let e=this._parentDialog;return e?e._getAfterAllClosed():this._afterAllClosedAtThisLevel}afterAllClosed=hn(()=>this.openDialogs.length?this._getAfterAllClosed():this._getAfterAllClosed().pipe(nt(void 0)));constructor(){this._dialogRefConstructor=As,this._dialogContainerType=r1,this._dialogDataToken=s1}open(e,i){let r;i=v(v({},this._defaultOptions||new df),i),i.id=i.id||this._idGenerator.getId("mat-mdc-dialog-"),i.scrollStrategy=i.scrollStrategy||this._scrollStrategy();let o=this._dialog.open(e,N(v({},i),{positionStrategy:Yn(this._injector).centerHorizontally().centerVertically(),disableClose:!0,closePredicate:void 0,closeOnDestroy:!1,closeOnOverlayDetachments:!1,disableAnimations:this._animationsDisabled||i.enterAnimationDuration?.toLocaleString()==="0"||i.exitAnimationDuration?.toString()==="0",container:{type:this._dialogContainerType,providers:()=>[{provide:this.dialogConfigClass,useValue:i},{provide:sr,useValue:i}]},templateContext:()=>({dialogRef:r}),providers:(s,a,l)=>(r=new this._dialogRefConstructor(s,i,l),r.updatePosition(i?.position),[{provide:this._dialogContainerType,useValue:l},{provide:this._dialogDataToken,useValue:a.data},{provide:this._dialogRefConstructor,useValue:r},{provide:vo,useValue:null}])}));return r.componentRef=o.componentRef,r.componentInstance=o.componentInstance,this.openDialogs.push(r),this.afterOpened.next(r),r.afterClosed().subscribe(()=>{let s=this.openDialogs.indexOf(r);s>-1&&(this.openDialogs.splice(s,1),this.openDialogs.length||this._getAfterAllClosed().next())}),r}closeAll(){this._closeDialogs(this.openDialogs)}getDialogById(e){return this.openDialogs.find(i=>i.id===e)}ngOnDestroy(){this._closeDialogs(this._openDialogsAtThisLevel),this._afterAllClosedAtThisLevel.complete(),this._afterOpenedAtThisLevel.complete()}_closeDialogs(e){let i=e.length;for(;i--;)e[i].close()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();var yE=(()=>{class t{_dialogRef=u(As,{optional:!0});_elementRef=u(O);_dialog=u(Rs);ngOnInit(){this._dialogRef||(this._dialogRef=c1(this._elementRef,this._dialog.openDialogs)),this._dialogRef&&Promise.resolve().then(()=>{this._onAdd()})}ngOnDestroy(){this._dialogRef?._containerInstance&&Promise.resolve().then(()=>{this._onRemove()})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=z({type:t})}return t})(),SE=(()=>{class t extends yE{id=u(Ie).getId("mat-mdc-dialog-title-");_onAdd(){this._dialogRef._containerInstance?._addAriaLabelledBy?.(this.id)}_onRemove(){this._dialogRef?._containerInstance?._removeAriaLabelledBy?.(this.id)}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ht(t)))(r||t)}})();static \u0275dir=z({type:t,selectors:[["","mat-dialog-title",""],["","matDialogTitle",""]],hostAttrs:[1,"mat-mdc-dialog-title","mdc-dialog__title"],hostVars:1,hostBindings:function(i,r){i&2&&Et("id",r.id)},inputs:{id:"id"},exportAs:["matDialogTitle"],features:[Te]})}return t})(),CE=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=z({type:t,selectors:[["","mat-dialog-content",""],["mat-dialog-content"],["","matDialogContent",""]],hostAttrs:[1,"mat-mdc-dialog-content","mdc-dialog__content"],features:[Bp([ov])]})}return t})(),DE=(()=>{class t extends yE{align;_onAdd(){this._dialogRef._containerInstance?._updateActionSectionCount?.(1)}_onRemove(){this._dialogRef._containerInstance?._updateActionSectionCount?.(-1)}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ht(t)))(r||t)}})();static \u0275dir=z({type:t,selectors:[["","mat-dialog-actions",""],["mat-dialog-actions"],["","matDialogActions",""]],hostAttrs:[1,"mat-mdc-dialog-actions","mdc-dialog__actions"],hostVars:6,hostBindings:function(i,r){i&2&&B("mat-mdc-dialog-actions-align-start",r.align==="start")("mat-mdc-dialog-actions-align-center",r.align==="center")("mat-mdc-dialog-actions-align-end",r.align==="end")},inputs:{align:"align"},features:[Te]})}return t})();function c1(t,n){let e=t.nativeElement.parentElement;for(;e&&!e.classList.contains("mat-mdc-dialog-container");)e=e.parentElement;return e?n.find(i=>i.id===e.id):null}var EE=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({providers:[Rs],imports:[fv,an,_i,ve]})}return t})();var uf=(()=>{class t{get vertical(){return this._vertical}set vertical(e){this._vertical=ln(e)}_vertical=!1;get inset(){return this._inset}set inset(e){this._inset=ln(e)}_inset=!1;static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-divider"]],hostAttrs:["role","separator",1,"mat-divider"],hostVars:7,hostBindings:function(i,r){i&2&&(re("aria-orientation",r.vertical?"vertical":"horizontal"),B("mat-divider-vertical",r.vertical)("mat-divider-horizontal",!r.vertical)("mat-divider-inset",r.inset))},inputs:{vertical:"vertical",inset:"inset"},decls:0,vars:0,template:function(i,r){},styles:[`.mat-divider {
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
`],encapsulation:2})}return t})(),ff=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({imports:[ve]})}return t})();var xv=class{_box;_destroyed=new x;_resizeSubject=new x;_resizeObserver;_elementObservables=new Map;constructor(n){this._box=n,typeof ResizeObserver<"u"&&(this._resizeObserver=new ResizeObserver(e=>this._resizeSubject.next(e)))}observe(n){return this._elementObservables.has(n)||this._elementObservables.set(n,new ne(e=>{let i=this._resizeSubject.subscribe(e);return this._resizeObserver?.observe(n,{box:this._box}),()=>{this._resizeObserver?.unobserve(n),i.unsubscribe(),this._elementObservables.delete(n)}}).pipe(ge(e=>e.some(i=>i.target===n)),Vc({bufferSize:1,refCount:!0}),Ce(this._destroyed))),this._elementObservables.get(n)}destroy(){this._destroyed.next(),this._destroyed.complete(),this._resizeSubject.complete(),this._elementObservables.clear()}},wE=(()=>{class t{_cleanupErrorListener;_observers=new Map;_ngZone=u(k);constructor(){typeof ResizeObserver<"u"}ngOnDestroy(){for(let[,e]of this._observers)e.destroy();this._observers.clear(),this._cleanupErrorListener?.()}observe(e,i){let r=i?.box||"content-box";return this._observers.has(r)||this._observers.set(r,new xv(r)),this._observers.get(r).observe(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();var u1=["notch"],f1=["*"],IE=["iconPrefixContainer"],ME=["textPrefixContainer"],TE=["iconSuffixContainer"],NE=["textSuffixContainer"],h1=["textField"],m1=["*",[["mat-label"]],[["","matPrefix",""],["","matIconPrefix",""]],[["","matTextPrefix",""]],[["","matTextSuffix",""]],[["","matSuffix",""],["","matIconSuffix",""]],[["mat-error"],["","matError",""]],[["mat-hint",3,"align","end"]],[["mat-hint","align","end"]]],p1=["*","mat-label","[matPrefix], [matIconPrefix]","[matTextPrefix]","[matTextSuffix]","[matSuffix], [matIconSuffix]","mat-error, [matError]","mat-hint:not([align='end'])","mat-hint[align='end']"];function g1(t,n){t&1&&ie(0,"span",21)}function v1(t,n){if(t&1&&(m(0,"label",20),me(1,1),P(2,g1,1,0,"span",21),g()),t&2){let e=C(2);L("floating",e._shouldLabelFloat())("monitorResize",e._hasOutline())("id",e._labelId),re("for",e._control.disableAutomaticLabeling?null:e._control.id),_(2),F(!e.hideRequiredMarker&&e._control.required?2:-1)}}function b1(t,n){if(t&1&&P(0,v1,3,5,"label",20),t&2){let e=C();F(e._hasFloatingLabel()?0:-1)}}function _1(t,n){t&1&&ie(0,"div",7)}function y1(t,n){}function S1(t,n){if(t&1&&Dt(0,y1,0,0,"ng-template",13),t&2){C(2);let e=ht(1);L("ngTemplateOutlet",e)}}function C1(t,n){if(t&1&&(m(0,"div",9),P(1,S1,1,1,null,13),g()),t&2){let e=C();L("matFormFieldNotchedOutlineOpen",e._shouldLabelFloat()),_(),F(e._forceDisplayInfixLabel()?-1:1)}}function D1(t,n){t&1&&(m(0,"div",10,2),me(2,2),g())}function E1(t,n){t&1&&(m(0,"div",11,3),me(2,3),g())}function x1(t,n){}function w1(t,n){if(t&1&&Dt(0,x1,0,0,"ng-template",13),t&2){C();let e=ht(1);L("ngTemplateOutlet",e)}}function I1(t,n){t&1&&(m(0,"div",14,4),me(2,4),g())}function M1(t,n){t&1&&(m(0,"div",15,5),me(2,5),g())}function T1(t,n){t&1&&ie(0,"div",16)}function N1(t,n){t&1&&(m(0,"div",18),me(1,6),g())}function k1(t,n){if(t&1&&(m(0,"mat-hint",22),y(1),g()),t&2){let e=C(2);L("id",e._hintLabelId),_(),ye(e.hintLabel)}}function A1(t,n){if(t&1&&(m(0,"div",19),P(1,k1,2,2,"mat-hint",22),me(2,7),ie(3,"div",23),me(4,8),g()),t&2){let e=C();_(),F(e.hintLabel?1:-1)}}var xi=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=z({type:t,selectors:[["mat-label"]]})}return t})(),LE=new b("MatError"),Iv=(()=>{class t{id=u(Ie).getId("mat-mdc-error-");static \u0275fac=function(i){return new(i||t)};static \u0275dir=z({type:t,selectors:[["mat-error"],["","matError",""]],hostAttrs:[1,"mat-mdc-form-field-error","mat-mdc-form-field-bottom-align"],hostVars:1,hostBindings:function(i,r){i&2&&Et("id",r.id)},inputs:{id:"id"},features:[Be([{provide:LE,useExisting:t}])]})}return t})(),wv=(()=>{class t{align="start";id=u(Ie).getId("mat-mdc-hint-");static \u0275fac=function(i){return new(i||t)};static \u0275dir=z({type:t,selectors:[["mat-hint"]],hostAttrs:[1,"mat-mdc-form-field-hint","mat-mdc-form-field-bottom-align"],hostVars:4,hostBindings:function(i,r){i&2&&(Et("id",r.id),re("align",null),B("mat-mdc-form-field-hint-end",r.align==="end"))},inputs:{align:"align",id:"id"}})}return t})(),R1=new b("MatPrefix");var O1=new b("MatSuffix");var VE=new b("FloatingLabelParent"),kE=(()=>{class t{_elementRef=u(O);get floating(){return this._floating}set floating(e){this._floating=e,this.monitorResize&&this._handleResize()}_floating=!1;get monitorResize(){return this._monitorResize}set monitorResize(e){this._monitorResize=e,this._monitorResize?this._subscribeToResize():this._resizeSubscription.unsubscribe()}_monitorResize=!1;_resizeObserver=u(wE);_ngZone=u(k);_parent=u(VE);_resizeSubscription=new pe;ngOnDestroy(){this._resizeSubscription.unsubscribe()}getWidth(){return P1(this._elementRef.nativeElement)}get element(){return this._elementRef.nativeElement}_handleResize(){setTimeout(()=>this._parent._handleLabelResized())}_subscribeToResize(){this._resizeSubscription.unsubscribe(),this._ngZone.runOutsideAngular(()=>{this._resizeSubscription=this._resizeObserver.observe(this._elementRef.nativeElement,{box:"border-box"}).subscribe(()=>this._handleResize())})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=z({type:t,selectors:[["label","matFormFieldFloatingLabel",""]],hostAttrs:[1,"mdc-floating-label","mat-mdc-floating-label"],hostVars:2,hostBindings:function(i,r){i&2&&B("mdc-floating-label--float-above",r.floating)},inputs:{floating:"floating",monitorResize:"monitorResize"}})}return t})();function P1(t){let n=t;if(n.offsetParent!==null)return n.scrollWidth;let e=n.cloneNode(!0);e.style.setProperty("position","absolute"),e.style.setProperty("transform","translate(-9999px, -9999px)"),document.documentElement.appendChild(e);let i=e.scrollWidth;return e.remove(),i}var AE="mdc-line-ripple--active",hf="mdc-line-ripple--deactivating",RE=(()=>{class t{_elementRef=u(O);_cleanupTransitionEnd;constructor(){let e=u(k),i=u(Re);e.runOutsideAngular(()=>{this._cleanupTransitionEnd=i.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionEnd)})}activate(){let e=this._elementRef.nativeElement.classList;e.remove(hf),e.add(AE)}deactivate(){this._elementRef.nativeElement.classList.add(hf)}_handleTransitionEnd=e=>{let i=this._elementRef.nativeElement.classList,r=i.contains(hf);e.propertyName==="opacity"&&r&&i.remove(AE,hf)};ngOnDestroy(){this._cleanupTransitionEnd()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=z({type:t,selectors:[["div","matFormFieldLineRipple",""]],hostAttrs:[1,"mdc-line-ripple"]})}return t})(),OE=(()=>{class t{_elementRef=u(O);_ngZone=u(k);open=!1;_notch;ngAfterViewInit(){let e=this._elementRef.nativeElement,i=e.querySelector(".mdc-floating-label");i?(e.classList.add("mdc-notched-outline--upgraded"),typeof requestAnimationFrame=="function"&&(i.style.transitionDuration="0s",this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>i.style.transitionDuration="")}))):e.classList.add("mdc-notched-outline--no-label")}_setNotchWidth(e){let i=this._notch.nativeElement;!this.open||!e?i.style.width="":i.style.width=`calc(${e}px * var(--mat-mdc-form-field-floating-label-scale, 0.75) + 9px)`}_setMaxWidth(e){this._notch.nativeElement.style.setProperty("--mat-form-field-notch-max-width",`calc(100% - ${e}px)`)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["div","matFormFieldNotchedOutline",""]],viewQuery:function(i,r){if(i&1&&Ue(u1,5),i&2){let o;ce(o=de())&&(r._notch=o.first)}},hostAttrs:[1,"mdc-notched-outline"],hostVars:2,hostBindings:function(i,r){i&2&&B("mdc-notched-outline--notched",r.open)},inputs:{open:[0,"matFormFieldNotchedOutlineOpen","open"]},ngContentSelectors:f1,decls:5,vars:0,consts:[["notch",""],[1,"mat-mdc-notch-piece","mdc-notched-outline__leading"],[1,"mat-mdc-notch-piece","mdc-notched-outline__notch"],[1,"mat-mdc-notch-piece","mdc-notched-outline__trailing"]],template:function(i,r){i&1&&(Pe(),tn(0,"div",1),lt(1,"div",2,0),me(3),_t(),tn(4,"div",3))},encapsulation:2})}return t})(),Ll=(()=>{class t{value=null;stateChanges;id;placeholder;ngControl=null;focused=!1;empty=!1;shouldLabelFloat=!1;required=!1;disabled=!1;errorState=!1;controlType;autofilled;userAriaDescribedBy;disableAutomaticLabeling;describedByIds;static \u0275fac=function(i){return new(i||t)};static \u0275dir=z({type:t})}return t})();var Vl=new b("MatFormField"),F1=new b("MAT_FORM_FIELD_DEFAULT_OPTIONS"),PE="fill",L1="auto",FE="fixed",V1="translateY(-50%)",dr=(()=>{class t{_elementRef=u(O);_changeDetectorRef=u(Oe);_platform=u(be);_idGenerator=u(Ie);_ngZone=u(k);_defaults=u(F1,{optional:!0});_currentDirection;_textField;_iconPrefixContainer;_textPrefixContainer;_iconSuffixContainer;_textSuffixContainer;_floatingLabel;_notchedOutline;_lineRipple;_iconPrefixContainerSignal=ui("iconPrefixContainer");_textPrefixContainerSignal=ui("textPrefixContainer");_iconSuffixContainerSignal=ui("iconSuffixContainer");_textSuffixContainerSignal=ui("textSuffixContainer");_prefixSuffixContainers=K(()=>[this._iconPrefixContainerSignal(),this._textPrefixContainerSignal(),this._iconSuffixContainerSignal(),this._textSuffixContainerSignal()].map(e=>e?.nativeElement).filter(e=>e!==void 0));_formFieldControl;_prefixChildren;_suffixChildren;_errorChildren;_hintChildren;_labelChild=IS(xi);get hideRequiredMarker(){return this._hideRequiredMarker}set hideRequiredMarker(e){this._hideRequiredMarker=ln(e)}_hideRequiredMarker=!1;color="primary";get floatLabel(){return this._floatLabel||this._defaults?.floatLabel||L1}set floatLabel(e){e!==this._floatLabel&&(this._floatLabel=e,this._changeDetectorRef.markForCheck())}_floatLabel;get appearance(){return this._appearanceSignal()}set appearance(e){let i=e||this._defaults?.appearance||PE;this._appearanceSignal.set(i)}_appearanceSignal=U(PE);get subscriptSizing(){return this._subscriptSizing||this._defaults?.subscriptSizing||FE}set subscriptSizing(e){this._subscriptSizing=e||this._defaults?.subscriptSizing||FE}_subscriptSizing=null;get hintLabel(){return this._hintLabel}set hintLabel(e){this._hintLabel=e,this._processHints()}_hintLabel="";_hasIconPrefix=!1;_hasTextPrefix=!1;_hasIconSuffix=!1;_hasTextSuffix=!1;_labelId=this._idGenerator.getId("mat-mdc-form-field-label-");_hintLabelId=this._idGenerator.getId("mat-mdc-hint-");_describedByIds;get _control(){return this._explicitFormFieldControl||this._formFieldControl}set _control(e){this._explicitFormFieldControl=e}_destroyed=new x;_isFocused=null;_explicitFormFieldControl;_previousControl=null;_previousControlValidatorFn=null;_stateChanges;_valueChanges;_describedByChanges;_outlineLabelOffsetResizeObserver=null;_animationsDisabled=Ee();constructor(){let e=this._defaults,i=u(ct);e&&(e.appearance&&(this.appearance=e.appearance),this._hideRequiredMarker=!!e?.hideRequiredMarker,e.color&&(this.color=e.color)),ut(()=>this._currentDirection=i.valueSignal()),this._syncOutlineLabelOffset()}ngAfterViewInit(){this._updateFocusState(),this._animationsDisabled||this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-form-field-animations-enabled")},300)}),this._changeDetectorRef.detectChanges()}ngAfterContentInit(){this._assertFormFieldControl(),this._initializeSubscript(),this._initializePrefixAndSuffix()}ngAfterContentChecked(){this._assertFormFieldControl(),this._control!==this._previousControl&&(this._initializeControl(this._previousControl),this._control.ngControl&&this._control.ngControl.control&&(this._previousControlValidatorFn=this._control.ngControl.control.validator),this._previousControl=this._control,this._changeDetectorRef.markForCheck()),this._control.ngControl&&this._control.ngControl.control&&this._control.ngControl.control.validator!==this._previousControlValidatorFn&&this._changeDetectorRef.markForCheck()}ngOnDestroy(){this._outlineLabelOffsetResizeObserver?.disconnect(),this._stateChanges?.unsubscribe(),this._valueChanges?.unsubscribe(),this._describedByChanges?.unsubscribe(),this._destroyed.next(),this._destroyed.complete()}getLabelId=K(()=>this._hasFloatingLabel()?this._labelId:null);getConnectedOverlayOrigin(){return this._textField||this._elementRef}_animateAndLockLabel(){this._hasFloatingLabel()&&(this.floatLabel="always")}_initializeControl(e){let i=this._control,r="mat-mdc-form-field-type-";e&&this._elementRef.nativeElement.classList.remove(r+e.controlType),i.controlType&&this._elementRef.nativeElement.classList.add(r+i.controlType),this._stateChanges?.unsubscribe(),this._stateChanges=i.stateChanges.subscribe(()=>{this._updateFocusState(),this._changeDetectorRef.markForCheck()}),this._describedByChanges?.unsubscribe(),this._describedByChanges=i.stateChanges.pipe(nt([void 0,void 0]),oe(()=>[i.errorState,i.userAriaDescribedBy]),Lc(),ge(([[o,s],[a,l]])=>o!==a||s!==l)).subscribe(()=>this._syncDescribedByIds()),this._valueChanges?.unsubscribe(),i.ngControl&&i.ngControl.valueChanges&&(this._valueChanges=i.ngControl.valueChanges.pipe(Ce(this._destroyed)).subscribe(()=>this._changeDetectorRef.markForCheck()))}_checkPrefixAndSuffixTypes(){this._hasIconPrefix=!!this._prefixChildren.find(e=>!e._isText),this._hasTextPrefix=!!this._prefixChildren.find(e=>e._isText),this._hasIconSuffix=!!this._suffixChildren.find(e=>!e._isText),this._hasTextSuffix=!!this._suffixChildren.find(e=>e._isText)}_initializePrefixAndSuffix(){this._checkPrefixAndSuffixTypes(),Nt(this._prefixChildren.changes,this._suffixChildren.changes).subscribe(()=>{this._checkPrefixAndSuffixTypes(),this._changeDetectorRef.markForCheck()})}_initializeSubscript(){this._hintChildren.changes.subscribe(()=>{this._processHints(),this._changeDetectorRef.markForCheck()}),this._errorChildren.changes.subscribe(()=>{this._syncDescribedByIds(),this._changeDetectorRef.markForCheck()}),this._validateHints(),this._syncDescribedByIds()}_assertFormFieldControl(){this._control}_updateFocusState(){let e=this._control.focused;e&&!this._isFocused?(this._isFocused=!0,this._lineRipple?.activate()):!e&&(this._isFocused||this._isFocused===null)&&(this._isFocused=!1,this._lineRipple?.deactivate()),this._elementRef.nativeElement.classList.toggle("mat-focused",e),this._textField?.nativeElement.classList.toggle("mdc-text-field--focused",e)}_syncOutlineLabelOffset(){Kr({earlyRead:()=>{if(this._appearanceSignal()!=="outline")return this._outlineLabelOffsetResizeObserver?.disconnect(),null;if(globalThis.ResizeObserver){this._outlineLabelOffsetResizeObserver||=new globalThis.ResizeObserver(()=>{this._writeOutlinedLabelStyles(this._getOutlinedLabelOffset())});for(let e of this._prefixSuffixContainers())this._outlineLabelOffsetResizeObserver.observe(e,{box:"border-box"})}return this._getOutlinedLabelOffset()},write:e=>this._writeOutlinedLabelStyles(e())})}_shouldAlwaysFloat(){return this.floatLabel==="always"}_hasOutline(){return this.appearance==="outline"}_forceDisplayInfixLabel(){return!this._platform.isBrowser&&this._prefixChildren.length&&!this._shouldLabelFloat()}_hasFloatingLabel=K(()=>!!this._labelChild());_shouldLabelFloat(){return this._hasFloatingLabel()?this._control.shouldLabelFloat||this._shouldAlwaysFloat():!1}_shouldForward(e){let i=this._control?this._control.ngControl:null;return i&&i[e]}_getSubscriptMessageType(){return this._errorChildren&&this._errorChildren.length>0&&this._control.errorState?"error":"hint"}_handleLabelResized(){this._refreshOutlineNotchWidth()}_refreshOutlineNotchWidth(){!this._hasOutline()||!this._floatingLabel||!this._shouldLabelFloat()?this._notchedOutline?._setNotchWidth(0):this._notchedOutline?._setNotchWidth(this._floatingLabel.getWidth())}_processHints(){this._validateHints(),this._syncDescribedByIds()}_validateHints(){this._hintChildren}_syncDescribedByIds(){if(this._control){let e=[];if(this._control.userAriaDescribedBy&&typeof this._control.userAriaDescribedBy=="string"&&e.push(...this._control.userAriaDescribedBy.split(" ")),this._getSubscriptMessageType()==="hint"){let o=this._hintChildren?this._hintChildren.find(a=>a.align==="start"):null,s=this._hintChildren?this._hintChildren.find(a=>a.align==="end"):null;o?e.push(o.id):this._hintLabel&&e.push(this._hintLabelId),s&&e.push(s.id)}else this._errorChildren&&e.push(...this._errorChildren.map(o=>o.id));let i=this._control.describedByIds,r;if(i){let o=this._describedByIds||e;r=e.concat(i.filter(s=>s&&!o.includes(s)))}else r=e;this._control.setDescribedByIds(r),this._describedByIds=e}}_getOutlinedLabelOffset(){if(!this._hasOutline()||!this._floatingLabel)return null;if(!this._iconPrefixContainer&&!this._textPrefixContainer)return["",null];if(!this._isAttachedToDom())return null;let e=this._iconPrefixContainer?.nativeElement,i=this._textPrefixContainer?.nativeElement,r=this._iconSuffixContainer?.nativeElement,o=this._textSuffixContainer?.nativeElement,s=e?.getBoundingClientRect().width??0,a=i?.getBoundingClientRect().width??0,l=r?.getBoundingClientRect().width??0,c=o?.getBoundingClientRect().width??0,d=this._currentDirection==="rtl"?"-1":"1",f=`${s+a}px`,p=`calc(${d} * (${f} + var(--mat-mdc-form-field-label-offset-x, 0px)))`,S=`var(--mat-mdc-form-field-label-transform, ${V1} translateX(${p}))`,E=s+a+l+c;return[S,E]}_writeOutlinedLabelStyles(e){if(e!==null){let[i,r]=e;this._floatingLabel&&(this._floatingLabel.element.style.transform=i),r!==null&&this._notchedOutline?._setMaxWidth(r)}}_isAttachedToDom(){let e=this._elementRef.nativeElement;if(e.getRootNode){let i=e.getRootNode();return i&&i!==e}return document.documentElement.contains(e)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-form-field"]],contentQueries:function(i,r,o){if(i&1&&(eu(o,r._labelChild,xi,5),Gn(o,Ll,5)(o,R1,5)(o,O1,5)(o,LE,5)(o,wv,5)),i&2){qi();let s;ce(s=de())&&(r._formFieldControl=s.first),ce(s=de())&&(r._prefixChildren=s),ce(s=de())&&(r._suffixChildren=s),ce(s=de())&&(r._errorChildren=s),ce(s=de())&&(r._hintChildren=s)}},viewQuery:function(i,r){if(i&1&&(Wi(r._iconPrefixContainerSignal,IE,5)(r._textPrefixContainerSignal,ME,5)(r._iconSuffixContainerSignal,TE,5)(r._textSuffixContainerSignal,NE,5),Ue(h1,5)(IE,5)(ME,5)(TE,5)(NE,5)(kE,5)(OE,5)(RE,5)),i&2){qi(4);let o;ce(o=de())&&(r._textField=o.first),ce(o=de())&&(r._iconPrefixContainer=o.first),ce(o=de())&&(r._textPrefixContainer=o.first),ce(o=de())&&(r._iconSuffixContainer=o.first),ce(o=de())&&(r._textSuffixContainer=o.first),ce(o=de())&&(r._floatingLabel=o.first),ce(o=de())&&(r._notchedOutline=o.first),ce(o=de())&&(r._lineRipple=o.first)}},hostAttrs:[1,"mat-mdc-form-field"],hostVars:38,hostBindings:function(i,r){i&2&&B("mat-mdc-form-field-label-always-float",r._shouldAlwaysFloat())("mat-mdc-form-field-has-icon-prefix",r._hasIconPrefix)("mat-mdc-form-field-has-icon-suffix",r._hasIconSuffix)("mat-form-field-invalid",r._control.errorState)("mat-form-field-disabled",r._control.disabled)("mat-form-field-autofilled",r._control.autofilled)("mat-form-field-appearance-fill",r.appearance=="fill")("mat-form-field-appearance-outline",r.appearance=="outline")("mat-form-field-hide-placeholder",r._hasFloatingLabel()&&!r._shouldLabelFloat())("mat-primary",r.color!=="accent"&&r.color!=="warn")("mat-accent",r.color==="accent")("mat-warn",r.color==="warn")("ng-untouched",r._shouldForward("untouched"))("ng-touched",r._shouldForward("touched"))("ng-pristine",r._shouldForward("pristine"))("ng-dirty",r._shouldForward("dirty"))("ng-valid",r._shouldForward("valid"))("ng-invalid",r._shouldForward("invalid"))("ng-pending",r._shouldForward("pending"))},inputs:{hideRequiredMarker:"hideRequiredMarker",color:"color",floatLabel:"floatLabel",appearance:"appearance",subscriptSizing:"subscriptSizing",hintLabel:"hintLabel"},exportAs:["matFormField"],features:[Be([{provide:Vl,useExisting:t},{provide:VE,useExisting:t}])],ngContentSelectors:p1,decls:18,vars:21,consts:[["labelTemplate",""],["textField",""],["iconPrefixContainer",""],["textPrefixContainer",""],["textSuffixContainer",""],["iconSuffixContainer",""],[1,"mat-mdc-text-field-wrapper","mdc-text-field",3,"click"],[1,"mat-mdc-form-field-focus-overlay"],[1,"mat-mdc-form-field-flex"],["matFormFieldNotchedOutline","",3,"matFormFieldNotchedOutlineOpen"],[1,"mat-mdc-form-field-icon-prefix"],[1,"mat-mdc-form-field-text-prefix"],[1,"mat-mdc-form-field-infix"],[3,"ngTemplateOutlet"],[1,"mat-mdc-form-field-text-suffix"],[1,"mat-mdc-form-field-icon-suffix"],["matFormFieldLineRipple",""],["aria-atomic","true","aria-live","polite",1,"mat-mdc-form-field-subscript-wrapper","mat-mdc-form-field-bottom-align"],[1,"mat-mdc-form-field-error-wrapper"],[1,"mat-mdc-form-field-hint-wrapper"],["matFormFieldFloatingLabel","",3,"floating","monitorResize","id"],["aria-hidden","true",1,"mat-mdc-form-field-required-marker","mdc-floating-label--required"],[3,"id"],[1,"mat-mdc-form-field-hint-spacer"]],template:function(i,r){if(i&1&&(Pe(m1),Dt(0,b1,1,1,"ng-template",null,0,$p),m(2,"div",6,1),D("click",function(s){return r._control.onContainerClick(s)}),P(4,_1,1,0,"div",7),m(5,"div",8),P(6,C1,2,2,"div",9),P(7,D1,3,0,"div",10),P(8,E1,3,0,"div",11),m(9,"div",12),P(10,w1,1,1,null,13),me(11),g(),P(12,I1,3,0,"div",14),P(13,M1,3,0,"div",15),g(),P(14,T1,1,0,"div",16),g(),m(15,"div",17),P(16,N1,2,0,"div",18)(17,A1,5,1,"div",19),g()),i&2){let o;_(2),B("mdc-text-field--filled",!r._hasOutline())("mdc-text-field--outlined",r._hasOutline())("mdc-text-field--no-label",!r._hasFloatingLabel())("mdc-text-field--disabled",r._control.disabled)("mdc-text-field--invalid",r._control.errorState),_(2),F(!r._hasOutline()&&!r._control.disabled?4:-1),_(2),F(r._hasOutline()?6:-1),_(),F(r._hasIconPrefix?7:-1),_(),F(r._hasTextPrefix?8:-1),_(2),F(!r._hasOutline()||r._forceDisplayInfixLabel()?10:-1),_(2),F(r._hasTextSuffix?12:-1),_(),F(r._hasIconSuffix?13:-1),_(),F(r._hasOutline()?-1:14),_(),B("mat-mdc-form-field-subscript-dynamic-size",r.subscriptSizing==="dynamic");let s=r._getSubscriptMessageType();_(),F((o=s)==="error"?16:o==="hint"?17:-1)}},dependencies:[kE,OE,Kp,RE,wv],styles:[`.mdc-text-field {
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
`],encapsulation:2})}return t})();var kn=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({imports:[qu,dr,ve]})}return t})();function BE(t){return Error(`Unable to find icon with the name "${t}"`)}function B1(){return Error("Could not find HttpClient for use with Angular Material icons. Please add provideHttpClient() to your providers.")}function jE(t){return Error(`The URL provided to MatIconRegistry was not trusted as a resource URL via Angular's DomSanitizer. Attempted URL was "${t}".`)}function HE(t){return Error(`The literal provided to MatIconRegistry was not trusted as safe HTML by Angular's DomSanitizer. Attempted literal was "${t}".`)}var wi=class{url;svgText;options;svgElement=null;constructor(n,e,i){this.url=n,this.svgText=e,this.options=i}},zE=(()=>{class t{_httpClient;_sanitizer;_errorHandler;_document;_svgIconConfigs=new Map;_iconSetConfigs=new Map;_cachedIconsByUrl=new Map;_inProgressUrlFetches=new Map;_fontCssClassesByAlias=new Map;_resolvers=[];_defaultFontSetClass=["material-icons","mat-ligature-font"];constructor(e,i,r,o){this._httpClient=e,this._sanitizer=i,this._errorHandler=o,this._document=r}addSvgIcon(e,i,r){return this.addSvgIconInNamespace("",e,i,r)}addSvgIconLiteral(e,i,r){return this.addSvgIconLiteralInNamespace("",e,i,r)}addSvgIconInNamespace(e,i,r,o){return this._addSvgIconConfig(e,i,new wi(r,null,o))}addSvgIconResolver(e){return this._resolvers.push(e),this}addSvgIconLiteralInNamespace(e,i,r,o){let s=this._sanitizer.sanitize(st.HTML,r);if(!s)throw HE(r);let a=uo(s);return this._addSvgIconConfig(e,i,new wi("",a,o))}addSvgIconSet(e,i){return this.addSvgIconSetInNamespace("",e,i)}addSvgIconSetLiteral(e,i){return this.addSvgIconSetLiteralInNamespace("",e,i)}addSvgIconSetInNamespace(e,i,r){return this._addSvgIconSetConfig(e,new wi(i,null,r))}addSvgIconSetLiteralInNamespace(e,i,r){let o=this._sanitizer.sanitize(st.HTML,i);if(!o)throw HE(i);let s=uo(o);return this._addSvgIconSetConfig(e,new wi("",s,r))}registerFontClassAlias(e,i=e){return this._fontCssClassesByAlias.set(e,i),this}classNameForFontAlias(e){return this._fontCssClassesByAlias.get(e)||e}setDefaultFontSetClass(...e){return this._defaultFontSetClass=e,this}getDefaultFontSetClass(){return this._defaultFontSetClass}getSvgIconFromUrl(e){let i=this._sanitizer.sanitize(st.RESOURCE_URL,e);if(!i)throw jE(e);let r=this._cachedIconsByUrl.get(i);return r?Y(mf(r)):this._loadSvgIconFromConfig(new wi(e,null)).pipe(dt(o=>this._cachedIconsByUrl.set(i,o)),oe(o=>mf(o)))}getNamedSvgIcon(e,i=""){let r=UE(i,e),o=this._svgIconConfigs.get(r);if(o)return this._getSvgFromConfig(o);if(o=this._getIconConfigFromResolvers(i,e),o)return this._svgIconConfigs.set(r,o),this._getSvgFromConfig(o);let s=this._iconSetConfigs.get(i);return s?this._getSvgFromIconSetConfigs(e,s):ta(BE(r))}ngOnDestroy(){this._resolvers=[],this._svgIconConfigs.clear(),this._iconSetConfigs.clear(),this._cachedIconsByUrl.clear()}_getSvgFromConfig(e){return e.svgText?Y(mf(this._svgElementFromConfig(e))):this._loadSvgIconFromConfig(e).pipe(oe(i=>mf(i)))}_getSvgFromIconSetConfigs(e,i){let r=this._extractIconWithNameFromAnySet(e,i);if(r)return Y(r);let o=i.filter(s=>!s.svgText).map(s=>this._loadSvgIconSetFromConfig(s).pipe(wr(a=>{let c=`Loading icon set URL: ${this._sanitizer.sanitize(st.RESOURCE_URL,s.url)} failed: ${a.message}`;return this._errorHandler.handleError(new Error(c)),Y(null)})));return ra(o).pipe(oe(()=>{let s=this._extractIconWithNameFromAnySet(e,i);if(!s)throw BE(e);return s}))}_extractIconWithNameFromAnySet(e,i){for(let r=i.length-1;r>=0;r--){let o=i[r];if(o.svgText&&o.svgText.toString().indexOf(e)>-1){let s=this._svgElementFromConfig(o),a=this._extractSvgIconFromSet(s,e,o.options);if(a)return a}}return null}_loadSvgIconFromConfig(e){return this._fetchIcon(e).pipe(dt(i=>e.svgText=i),oe(()=>this._svgElementFromConfig(e)))}_loadSvgIconSetFromConfig(e){return e.svgText?Y(null):this._fetchIcon(e).pipe(dt(i=>e.svgText=i))}_extractSvgIconFromSet(e,i,r){let o=e.querySelector(`[id="${i}"]`);if(!o)return null;let s=o.cloneNode(!0);if(s.removeAttribute("id"),s.nodeName.toLowerCase()==="svg")return this._setSvgAttributes(s,r);if(s.nodeName.toLowerCase()==="symbol")return this._setSvgAttributes(this._toSvgElement(s),r);let a=this._svgElementFromString(uo("<svg></svg>"));return a.appendChild(s),this._setSvgAttributes(a,r)}_svgElementFromString(e){let i=this._document.createElement("DIV");i.innerHTML=e;let r=i.querySelector("svg");if(!r)throw Error("<svg> tag not found");return r}_toSvgElement(e){let i=this._svgElementFromString(uo("<svg></svg>")),r=e.attributes;for(let o=0;o<r.length;o++){let{name:s,value:a}=r[o];s!=="id"&&i.setAttribute(s,a)}for(let o=0;o<e.childNodes.length;o++)e.childNodes[o].nodeType===this._document.ELEMENT_NODE&&i.appendChild(e.childNodes[o].cloneNode(!0));return i}_setSvgAttributes(e,i){return e.setAttribute("fit",""),e.setAttribute("height","100%"),e.setAttribute("width","100%"),e.setAttribute("preserveAspectRatio","xMidYMid meet"),e.setAttribute("focusable","false"),i&&i.viewBox&&e.setAttribute("viewBox",i.viewBox),e}_fetchIcon(e){let{url:i,options:r}=e,o=r?.withCredentials??!1;if(!this._httpClient)throw B1();if(i==null)throw Error(`Cannot fetch icon from URL "${i}".`);let s=this._sanitizer.sanitize(st.RESOURCE_URL,i);if(!s)throw jE(i);let a=this._inProgressUrlFetches.get(s);if(a)return a;let l=this._httpClient.get(s,{responseType:"text",withCredentials:o}).pipe(oe(c=>uo(c)),Ir(()=>this._inProgressUrlFetches.delete(s)),sa());return this._inProgressUrlFetches.set(s,l),l}_addSvgIconConfig(e,i,r){return this._svgIconConfigs.set(UE(e,i),r),this}_addSvgIconSetConfig(e,i){let r=this._iconSetConfigs.get(e);return r?r.push(i):this._iconSetConfigs.set(e,[i]),this}_svgElementFromConfig(e){if(!e.svgElement){let i=this._svgElementFromString(e.svgText);this._setSvgAttributes(i,e.options),e.svgElement=i}return e.svgElement}_getIconConfigFromResolvers(e,i){for(let r=0;r<this._resolvers.length;r++){let o=this._resolvers[r](i,e);if(o)return j1(o)?new wi(o.url,null,o.options):new wi(o,null)}}static \u0275fac=function(i){return new(i||t)(A(mg,8),A(Qa),A(j,8),A(Ot))};static \u0275prov=G({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function mf(t){return t.cloneNode(!0)}function UE(t,n){return t+":"+n}function j1(t){return!!(t.url&&t.options)}var H1=["*"],U1=new b("MAT_ICON_DEFAULT_OPTIONS"),z1=new b("mat-icon-location",{providedIn:"root",factory:()=>{let t=u(j),n=t?t.location:null;return{getPathname:()=>n?n.pathname+n.search:""}}}),$E=["clip-path","color-profile","src","cursor","fill","filter","marker","marker-start","marker-mid","marker-end","mask","stroke"],$1=$E.map(t=>`[${t}]`).join(", "),G1=/^url\(['"]?#(.*?)['"]?\)$/,dn=(()=>{class t{_elementRef=u(O);_iconRegistry=u(zE);_location=u(z1);_errorHandler=u(Ot);_defaultColor;get color(){return this._color||this._defaultColor}set color(e){this._color=e}_color;inline=!1;get svgIcon(){return this._svgIcon}set svgIcon(e){e!==this._svgIcon&&(e?this._updateSvgIcon(e):this._svgIcon&&this._clearSvgElement(),this._svgIcon=e)}_svgIcon;get fontSet(){return this._fontSet}set fontSet(e){let i=this._cleanupFontValue(e);i!==this._fontSet&&(this._fontSet=i,this._updateFontIconClasses())}_fontSet;get fontIcon(){return this._fontIcon}set fontIcon(e){let i=this._cleanupFontValue(e);i!==this._fontIcon&&(this._fontIcon=i,this._updateFontIconClasses())}_fontIcon;_previousFontSetClass=[];_previousFontIconClass;_svgName=null;_svgNamespace=null;_previousPath;_elementsWithExternalReferences;_currentIconFetch=pe.EMPTY;constructor(){let e=u(new Cn("aria-hidden"),{optional:!0}),i=u(U1,{optional:!0});i&&(i.color&&(this.color=this._defaultColor=i.color),i.fontSet&&(this.fontSet=i.fontSet)),e||this._elementRef.nativeElement.setAttribute("aria-hidden","true")}_splitIconName(e){if(!e)return["",""];let i=e.split(":");switch(i.length){case 1:return["",i[0]];case 2:return i;default:throw Error(`Invalid icon name: "${e}"`)}}ngOnInit(){this._updateFontIconClasses()}ngAfterViewChecked(){let e=this._elementsWithExternalReferences;if(e&&e.size){let i=this._location.getPathname();i!==this._previousPath&&(this._previousPath=i,this._prependPathToReferences(i))}}ngOnDestroy(){this._currentIconFetch.unsubscribe(),this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear()}_usingFontIcon(){return!this.svgIcon}_setSvgElement(e){this._clearSvgElement();let i=this._location.getPathname();this._previousPath=i,this._cacheChildrenWithExternalReferences(e),this._prependPathToReferences(i),this._elementRef.nativeElement.appendChild(e)}_clearSvgElement(){let e=this._elementRef.nativeElement,i=e.childNodes.length;for(this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear();i--;){let r=e.childNodes[i];(r.nodeType!==1||r.nodeName.toLowerCase()==="svg")&&r.remove()}}_updateFontIconClasses(){if(!this._usingFontIcon())return;let e=this._elementRef.nativeElement,i=(this.fontSet?this._iconRegistry.classNameForFontAlias(this.fontSet).split(/ +/):this._iconRegistry.getDefaultFontSetClass()).filter(r=>r.length>0);this._previousFontSetClass.forEach(r=>e.classList.remove(r)),i.forEach(r=>e.classList.add(r)),this._previousFontSetClass=i,this.fontIcon!==this._previousFontIconClass&&!i.includes("mat-ligature-font")&&(this._previousFontIconClass&&e.classList.remove(this._previousFontIconClass),this.fontIcon&&e.classList.add(this.fontIcon),this._previousFontIconClass=this.fontIcon)}_cleanupFontValue(e){return typeof e=="string"?e.trim().split(" ")[0]:e}_prependPathToReferences(e){let i=this._elementsWithExternalReferences;i&&i.forEach((r,o)=>{r.forEach(s=>{o.setAttribute(s.name,`url('${e}#${s.value}')`)})})}_cacheChildrenWithExternalReferences(e){let i=e.querySelectorAll($1),r=this._elementsWithExternalReferences=this._elementsWithExternalReferences||new Map;for(let o=0;o<i.length;o++)$E.forEach(s=>{let a=i[o],l=a.getAttribute(s),c=l?l.match(G1):null;if(c){let d=r.get(a);d||(d=[],r.set(a,d)),d.push({name:s,value:c[1]})}})}_updateSvgIcon(e){if(this._svgNamespace=null,this._svgName=null,this._currentIconFetch.unsubscribe(),e){let[i,r]=this._splitIconName(e);i&&(this._svgNamespace=i),r&&(this._svgName=r),this._currentIconFetch=this._iconRegistry.getNamedSvgIcon(r,i).pipe(Ae(1)).subscribe(o=>this._setSvgElement(o),o=>{let s=`Error retrieving icon ${i}:${r}! ${o.message}`;this._errorHandler.handleError(new Error(s))})}}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-icon"]],hostAttrs:["role","img",1,"mat-icon","notranslate"],hostVars:10,hostBindings:function(i,r){i&2&&(re("data-mat-icon-type",r._usingFontIcon()?"font":"svg")("data-mat-icon-name",r._svgName||r.fontIcon)("data-mat-icon-namespace",r._svgNamespace||r.fontSet)("fontIcon",r._usingFontIcon()?r.fontIcon:null),xt(r.color?"mat-"+r.color:""),B("mat-icon-inline",r.inline)("mat-icon-no-color",r.color!=="primary"&&r.color!=="accent"&&r.color!=="warn"))},inputs:{color:"color",inline:[2,"inline","inline",V],svgIcon:"svgIcon",fontSet:"fontSet",fontIcon:"fontIcon"},exportAs:["matIcon"],ngContentSelectors:H1,decls:1,vars:0,template:function(i,r){i&1&&(Pe(),me(0))},styles:[`mat-icon, mat-icon.mat-primary, mat-icon.mat-accent, mat-icon.mat-warn {
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
`],encapsulation:2})}return t})(),un=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({imports:[ve]})}return t})();var W1=[[["mat-icon"],["","matMenuItemIcon",""]],"*"],q1=["mat-icon, [matMenuItemIcon]","*"];function Y1(t,n){t&1&&(Vi(),m(0,"svg",2),ie(1,"polygon",3),g())}var Z1=["*"];function X1(t,n){if(t&1){let e=Le();lt(0,"div",0),ts("click",function(){J(e);let r=C();return ee(r.closed.emit("click"))})("animationstart",function(r){J(e);let o=C();return ee(o._onAnimationStart(r.animationName))})("animationend",function(r){J(e);let o=C();return ee(o._onAnimationDone(r.animationName))})("animationcancel",function(r){J(e);let o=C();return ee(o._onAnimationDone(r.animationName))}),lt(1,"div",1),me(2),_t()()}if(t&2){let e=C();xt(e._classList),B("mat-menu-panel-animations-disabled",e._animationsDisabled)("mat-menu-panel-exit-animation",e._panelAnimationState==="void")("mat-menu-panel-animating",e._isAnimating()),Et("id",e.panelId),re("aria-label",e.ariaLabel||null)("aria-labelledby",e.ariaLabelledby||null)("aria-describedby",e.ariaDescribedby||null)}}var Nv=new b("MAT_MENU_PANEL"),Bl=(()=>{class t{_elementRef=u(O);_document=u(j);_focusMonitor=u(sn);_parentMenu=u(Nv,{optional:!0});_changeDetectorRef=u(Oe);role="menuitem";disabled=!1;disableRipple=!1;_hovered=new x;_focused=new x;_highlighted=!1;_triggersSubmenu=!1;constructor(){u(ze).load(Zn),this._parentMenu?.addItem?.(this)}focus(e,i){this._focusMonitor&&e?this._focusMonitor.focusVia(this._getHostElement(),e,i):this._getHostElement().focus(i),this._focused.next(this)}ngAfterViewInit(){this._focusMonitor&&this._focusMonitor.monitor(this._elementRef,!1)}ngOnDestroy(){this._focusMonitor&&this._focusMonitor.stopMonitoring(this._elementRef),this._parentMenu&&this._parentMenu.removeItem&&this._parentMenu.removeItem(this),this._hovered.complete(),this._focused.complete()}_getTabIndex(){return this.disabled?"-1":"0"}_getHostElement(){return this._elementRef.nativeElement}_checkDisabled(e){this.disabled&&(e.preventDefault(),e.stopPropagation())}_handleMouseEnter(){this._hovered.next(this)}getLabel(){let e=this._elementRef.nativeElement.cloneNode(!0),i=e.querySelectorAll("mat-icon, .material-icons");for(let r=0;r<i.length;r++)i[r].remove();return e.textContent?.trim()||""}_setHighlighted(e){this._highlighted=e,this._changeDetectorRef.markForCheck()}_setTriggersSubmenu(e){this._triggersSubmenu=e,this._changeDetectorRef.markForCheck()}_hasFocus(){return this._document&&this._document.activeElement===this._getHostElement()}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["","mat-menu-item",""]],hostAttrs:[1,"mat-mdc-menu-item","mat-focus-indicator"],hostVars:8,hostBindings:function(i,r){i&1&&D("click",function(s){return r._checkDisabled(s)})("mouseenter",function(){return r._handleMouseEnter()}),i&2&&(re("role",r.role)("tabindex",r._getTabIndex())("aria-disabled",r.disabled)("disabled",r.disabled||null),B("mat-mdc-menu-item-highlighted",r._highlighted)("mat-mdc-menu-item-submenu-trigger",r._triggersSubmenu))},inputs:{role:"role",disabled:[2,"disabled","disabled",V],disableRipple:[2,"disableRipple","disableRipple",V]},exportAs:["matMenuItem"],ngContentSelectors:q1,decls:5,vars:3,consts:[[1,"mat-mdc-menu-item-text"],["matRipple","",1,"mat-mdc-menu-ripple",3,"matRippleDisabled","matRippleTrigger"],["viewBox","0 0 5 10","focusable","false","aria-hidden","true",1,"mat-mdc-menu-submenu-icon"],["points","0,0 5,5 0,10"]],template:function(i,r){i&1&&(Pe(W1),me(0),m(1,"span",0),me(2,1),g(),ie(3,"div",1),P(4,Y1,2,0,":svg:svg",2)),i&2&&(_(3),L("matRippleDisabled",r.disableRipple||r.disabled)("matRippleTrigger",r._getHostElement()),_(),F(r._triggersSubmenu?4:-1))},dependencies:[ar],encapsulation:2})}return t})();var K1=new b("MatMenuContent");var Q1=new b("mat-menu-default-options",{providedIn:"root",factory:()=>({overlapTrigger:!1,xPosition:"after",yPosition:"below",backdropClass:"cdk-overlay-transparent-backdrop"})}),Tv="_mat-menu-enter",pf="_mat-menu-exit",Fs=(()=>{class t{_elementRef=u(O);_changeDetectorRef=u(Oe);_injector=u(R);_keyManager;_xPosition;_yPosition;_firstItemFocusRef;_exitFallbackTimeout;_animationsDisabled=Ee();_allItems;_directDescendantItems=new ai;_classList={};_panelAnimationState="void";_animationDone=new x;_isAnimating=U(!1);parentMenu;direction;overlayPanelClass;backdropClass;ariaLabel;ariaLabelledby;ariaDescribedby;get xPosition(){return this._xPosition}set xPosition(e){this._xPosition=e,this.setPositionClasses()}get yPosition(){return this._yPosition}set yPosition(e){this._yPosition=e,this.setPositionClasses()}templateRef;items;lazyContent;overlapTrigger=!1;hasBackdrop;get panelClass(){return this._previousPanelClass}set panelClass(e){let i=this._previousPanelClass,r=v({},this._classList);i&&i.length&&i.split(" ").forEach(o=>{r[o]=!1}),this._previousPanelClass=e,e&&e.length&&(e.split(" ").forEach(o=>{r[o]=!0}),this._elementRef.nativeElement.className=""),this._classList=r}_previousPanelClass="";get classList(){return this.panelClass}set classList(e){this.panelClass=e}closed=new W;close=this.closed;panelId=u(Ie).getId("mat-menu-panel-");constructor(){let e=u(Q1);this.overlayPanelClass=e.overlayPanelClass||"",this._xPosition=e.xPosition,this._yPosition=e.yPosition,this.backdropClass=e.backdropClass,this.overlapTrigger=e.overlapTrigger,this.hasBackdrop=e.hasBackdrop}ngOnInit(){this.setPositionClasses()}ngAfterContentInit(){this._updateDirectDescendants(),this._keyManager=new Rl(this._directDescendantItems).withWrap().withTypeAhead().withHomeAndEnd(),this._keyManager.tabOut.subscribe(()=>this.closed.emit("tab")),this._directDescendantItems.changes.pipe(nt(this._directDescendantItems),Ke(e=>Nt(...e.map(i=>i._focused)))).subscribe(e=>this._keyManager.updateActiveItem(e)),this._directDescendantItems.changes.subscribe(e=>{let i=this._keyManager;if(this._panelAnimationState==="enter"&&i.activeItem?._hasFocus()){let r=e.toArray(),o=Math.max(0,Math.min(r.length-1,i.activeItemIndex||0));r[o]&&!r[o].disabled?i.setActiveItem(o):i.setNextItemActive()}})}ngOnDestroy(){this._keyManager?.destroy(),this._directDescendantItems.destroy(),this.closed.complete(),this._firstItemFocusRef?.destroy(),clearTimeout(this._exitFallbackTimeout)}_hovered(){return this._directDescendantItems.changes.pipe(nt(this._directDescendantItems),Ke(i=>Nt(...i.map(r=>r._hovered))))}addItem(e){}removeItem(e){}_handleKeydown(e){let i=e.keyCode,r=this._keyManager;switch(i){case 27:$e(e)||(e.preventDefault(),this.closed.emit("keydown"));break;case 37:this.parentMenu&&this.direction==="ltr"&&this.closed.emit("keydown");break;case 39:this.parentMenu&&this.direction==="rtl"&&this.closed.emit("keydown");break;default:(i===38||i===40)&&r.setFocusOrigin("keyboard"),r.onKeydown(e);return}}focusFirstItem(e="program"){this._firstItemFocusRef?.destroy(),this._firstItemFocusRef=Je(()=>{let i=this._resolvePanel();if(!i||!i.contains(document.activeElement)){let r=this._keyManager;r.setFocusOrigin(e).setFirstItemActive(),!r.activeItem&&i&&i.focus()}},{injector:this._injector})}resetActiveItem(){this._keyManager.setActiveItem(-1)}setElevation(e){}setPositionClasses(e=this.xPosition,i=this.yPosition){this._classList=N(v({},this._classList),{"mat-menu-before":e==="before","mat-menu-after":e==="after","mat-menu-above":i==="above","mat-menu-below":i==="below"}),this._changeDetectorRef.markForCheck()}_onAnimationDone(e){let i=e===pf;(i||e===Tv)&&(i&&(clearTimeout(this._exitFallbackTimeout),this._exitFallbackTimeout=void 0),this._animationDone.next(i?"void":"enter"),this._isAnimating.set(!1))}_onAnimationStart(e){(e===Tv||e===pf)&&this._isAnimating.set(!0)}_setIsOpen(e){if(this._panelAnimationState=e?"enter":"void",e){if(this._keyManager.activeItemIndex===0){let i=this._resolvePanel();i&&(i.scrollTop=0)}}else this._animationsDisabled||(this._exitFallbackTimeout=setTimeout(()=>this._onAnimationDone(pf),200));this._animationsDisabled&&setTimeout(()=>{this._onAnimationDone(e?Tv:pf)}),this._changeDetectorRef.markForCheck()}_updateDirectDescendants(){this._allItems.changes.pipe(nt(this._allItems)).subscribe(e=>{this._directDescendantItems.reset(e.filter(i=>i._parentMenu===this)),this._directDescendantItems.notifyOnChanges()})}_resolvePanel(){let e=null;return this._directDescendantItems.length&&(e=this._directDescendantItems.first._getHostElement().closest('[role="menu"]')),e}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-menu"]],contentQueries:function(i,r,o){if(i&1&&Gn(o,K1,5)(o,Bl,5)(o,Bl,4),i&2){let s;ce(s=de())&&(r.lazyContent=s.first),ce(s=de())&&(r._allItems=s),ce(s=de())&&(r.items=s)}},viewQuery:function(i,r){if(i&1&&Ue(Ct,5),i&2){let o;ce(o=de())&&(r.templateRef=o.first)}},hostVars:3,hostBindings:function(i,r){i&2&&re("aria-label",null)("aria-labelledby",null)("aria-describedby",null)},inputs:{backdropClass:"backdropClass",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],xPosition:"xPosition",yPosition:"yPosition",overlapTrigger:[2,"overlapTrigger","overlapTrigger",V],hasBackdrop:[2,"hasBackdrop","hasBackdrop",e=>e==null?null:V(e)],panelClass:[0,"class","panelClass"],classList:"classList"},outputs:{closed:"closed",close:"close"},exportAs:["matMenu"],features:[Be([{provide:Nv,useExisting:t}])],ngContentSelectors:Z1,decls:1,vars:0,consts:[["tabindex","-1","role","menu",1,"mat-mdc-menu-panel",3,"click","animationstart","animationend","animationcancel","id"],[1,"mat-mdc-menu-content"]],template:function(i,r){i&1&&(Pe(),Kd(0,X1,3,12,"ng-template"))},styles:[`mat-menu {
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
`],encapsulation:2})}return t})(),J1=new b("mat-menu-scroll-strategy",{providedIn:"root",factory:()=>{let t=u(R);return()=>Di(t)}});var Ps=new WeakMap,eF=(()=>{class t{_canHaveBackdrop;_element=u(O);_viewContainerRef=u(bt);_menuItemInstance=u(Bl,{optional:!0,self:!0});_dir=u(ct,{optional:!0});_focusMonitor=u(sn);_ngZone=u(k);_injector=u(R);_scrollStrategy=u(J1);_changeDetectorRef=u(Oe);_animationsDisabled=Ee();_portal;_overlayRef=null;_menuOpen=!1;_closingActionsSubscription=pe.EMPTY;_menuCloseSubscription=pe.EMPTY;_pendingRemoval;_parentMaterialMenu;_parentInnerPadding;_openedBy=void 0;get _menu(){return this._menuInternal}set _menu(e){e!==this._menuInternal&&(this._menuInternal=e,this._menuCloseSubscription.unsubscribe(),e?(this._parentMaterialMenu,this._menuCloseSubscription=e.close.subscribe(i=>{this._destroyMenu(i),(i==="click"||i==="tab")&&this._parentMaterialMenu&&this._parentMaterialMenu.closed.emit(i)})):this._destroyMenu(),this._menuItemInstance?._setTriggersSubmenu(this._triggersSubmenu()))}_menuInternal=null;constructor(e){this._canHaveBackdrop=e;let i=u(Nv,{optional:!0});this._parentMaterialMenu=i instanceof Fs?i:void 0}ngOnDestroy(){this._menu&&this._ownsMenu(this._menu)&&Ps.delete(this._menu),this._pendingRemoval?.unsubscribe(),this._menuCloseSubscription.unsubscribe(),this._closingActionsSubscription.unsubscribe(),this._overlayRef&&(this._overlayRef.dispose(),this._overlayRef=null)}get menuOpen(){return this._menuOpen}get dir(){return this._dir&&this._dir.value==="rtl"?"rtl":"ltr"}_triggersSubmenu(){return!!(this._menuItemInstance&&this._parentMaterialMenu&&this._menu)}_closeMenu(){this._menu?.close.emit()}_openMenu(e){if(this._triggerIsAriaDisabled())return;let i=this._menu;if(this._menuOpen||!i)return;this._pendingRemoval?.unsubscribe();let r=Ps.get(i);Ps.set(i,this),r&&r!==this&&r._closeMenu();let o=this._createOverlay(i),s=o.getConfig(),a=s.positionStrategy;this._setPosition(i,a),this._canHaveBackdrop?s.hasBackdrop=i.hasBackdrop==null?!this._triggersSubmenu():i.hasBackdrop:s.hasBackdrop=i.hasBackdrop??!1,o.hasAttached()||(o.attach(this._getPortal(i)),i.lazyContent?.attach(this.menuData)),this._closingActionsSubscription=this._menuClosingActions().subscribe(()=>this._closeMenu()),i.parentMenu=this._triggersSubmenu()?this._parentMaterialMenu:void 0,i.direction=this.dir,e&&i.focusFirstItem(this._openedBy||"program"),this._setIsMenuOpen(!0),i instanceof Fs&&(i._setIsOpen(!0),i._directDescendantItems.changes.pipe(Ce(i.close)).subscribe(()=>{a.withLockedPosition(!1).reapplyLastPosition(),a.withLockedPosition(!0)}))}focus(e,i){this._focusMonitor&&e?this._focusMonitor.focusVia(this._element,e,i):this._element.nativeElement.focus(i)}_destroyMenu(e){let i=this._overlayRef,r=this._menu;!i||!this.menuOpen||(this._closingActionsSubscription.unsubscribe(),this._pendingRemoval?.unsubscribe(),r instanceof Fs&&this._ownsMenu(r)?(this._pendingRemoval=r._animationDone.pipe(Ae(1)).subscribe(()=>{i.detach(),Ps.has(r)||r.lazyContent?.detach()}),r._setIsOpen(!1)):(i.detach(),r?.lazyContent?.detach()),r&&this._ownsMenu(r)&&Ps.delete(r),this.restoreFocus&&(e==="keydown"||!this._openedBy||!this._triggersSubmenu())&&this.focus(this._openedBy),this._openedBy=void 0,this._setIsMenuOpen(!1))}_setIsMenuOpen(e){e!==this._menuOpen&&(this._menuOpen=e,this._menuOpen?this.menuOpened.emit():this.menuClosed.emit(),this._triggersSubmenu()&&this._menuItemInstance._setHighlighted(e),this._changeDetectorRef.markForCheck())}_createOverlay(e){if(!this._overlayRef){let i=this._getOverlayConfig(e);this._subscribeToPositions(e,i.positionStrategy),this._overlayRef=Tn(this._injector,i),this._overlayRef.keydownEvents().subscribe(r=>{this._menu instanceof Fs&&this._menu._handleKeydown(r)})}return this._overlayRef}_getOverlayConfig(e){return new In({positionStrategy:go(this._injector,this._getOverlayOrigin()).withLockedPosition().withGrowAfterOpen().withTransformOriginOn(".mat-menu-panel, .mat-mdc-menu-panel"),backdropClass:e.backdropClass||"cdk-overlay-transparent-backdrop",panelClass:e.overlayPanelClass,scrollStrategy:this._scrollStrategy(),direction:this._dir||"ltr",disableAnimations:this._animationsDisabled})}_subscribeToPositions(e,i){e.setPositionClasses&&i.positionChanges.subscribe(r=>{this._ngZone.run(()=>{let o=r.connectionPair.overlayX==="start"?"after":"before",s=r.connectionPair.overlayY==="top"?"below":"above";e.setPositionClasses(o,s)})})}_setPosition(e,i){let[r,o]=e.xPosition==="before"?["end","start"]:["start","end"],[s,a]=e.yPosition==="above"?["bottom","top"]:["top","bottom"],[l,c]=[s,a],[d,f]=[r,o],h=0;if(this._triggersSubmenu()){if(f=r=e.xPosition==="before"?"start":"end",o=d=r==="end"?"start":"end",this._parentMaterialMenu){if(this._parentInnerPadding==null){let p=this._parentMaterialMenu.items.first;this._parentInnerPadding=p?p._getHostElement().offsetTop:0}h=s==="bottom"?this._parentInnerPadding:-this._parentInnerPadding}}else e.overlapTrigger||(l=s==="top"?"bottom":"top",c=a==="top"?"bottom":"top");i.withPositions([{originX:r,originY:l,overlayX:d,overlayY:s,offsetY:h},{originX:o,originY:l,overlayX:f,overlayY:s,offsetY:h},{originX:r,originY:c,overlayX:d,overlayY:a,offsetY:-h},{originX:o,originY:c,overlayX:f,overlayY:a,offsetY:-h}])}_menuClosingActions(){let e=this._getOutsideClickStream(this._overlayRef),i=this._overlayRef.detachments(),r=this._parentMaterialMenu?this._parentMaterialMenu.closed:Y(),o=this._parentMaterialMenu?this._parentMaterialMenu._hovered().pipe(ge(s=>this._menuOpen&&s!==this._menuItemInstance)):Y();return Nt(e,r,o,i)}_getPortal(e){return(!this._portal||this._portal.templateRef!==e.templateRef)&&(this._portal=new xn(e.templateRef,this._viewContainerRef)),this._portal}_ownsMenu(e){return Ps.get(e)===this}_triggerIsAriaDisabled(){return V(this._element.nativeElement.getAttribute("aria-disabled"))}static \u0275fac=function(i){Zd()};static \u0275dir=z({type:t})}return t})(),GE=(()=>{class t extends eF{_cleanupTouchstart;_hoverSubscription=pe.EMPTY;get _deprecatedMatMenuTriggerFor(){return this.menu}set _deprecatedMatMenuTriggerFor(e){this.menu=e}get menu(){return this._menu}set menu(e){this._menu=e}menuData;restoreFocus=!0;menuOpened=new W;onMenuOpen=this.menuOpened;menuClosed=new W;onMenuClose=this.menuClosed;constructor(){super(!0);let e=u(Re);this._cleanupTouchstart=e.listen(this._element.nativeElement,"touchstart",i=>{co(i)||(this._openedBy="touch")},{passive:!0})}triggersSubmenu(){return super._triggersSubmenu()}toggleMenu(){return this.menuOpen?this.closeMenu():this.openMenu()}openMenu(){this._openMenu(!0)}closeMenu(){this._closeMenu()}updatePosition(){this._overlayRef?.updatePosition()}ngAfterContentInit(){this._handleHover()}ngOnDestroy(){super.ngOnDestroy(),this._cleanupTouchstart(),this._hoverSubscription.unsubscribe()}_getOverlayOrigin(){return this._element}_getOutsideClickStream(e){return e.backdropClick()}_handleMousedown(e){lo(e)||(this._openedBy=e.button===0?"mouse":void 0,this.triggersSubmenu()&&e.preventDefault())}_handleKeydown(e){let i=e.keyCode;(i===13||i===32)&&(this._openedBy="keyboard"),this.triggersSubmenu()&&(i===39&&this.dir==="ltr"||i===37&&this.dir==="rtl")&&(this._openedBy="keyboard",this.openMenu())}_handleClick(e){this.triggersSubmenu()?(e.stopPropagation(),this.openMenu()):this.toggleMenu()}_handleHover(){this.triggersSubmenu()&&this._parentMaterialMenu&&(this._hoverSubscription=this._parentMaterialMenu._hovered().subscribe(e=>{e===this._menuItemInstance&&!e.disabled&&this._parentMaterialMenu?._panelAnimationState!=="void"&&(this._openedBy="mouse",this._openMenu(!1))}))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=z({type:t,selectors:[["","mat-menu-trigger-for",""],["","matMenuTriggerFor",""]],hostAttrs:[1,"mat-mdc-menu-trigger"],hostVars:3,hostBindings:function(i,r){i&1&&D("click",function(s){return r._handleClick(s)})("mousedown",function(s){return r._handleMousedown(s)})("keydown",function(s){return r._handleKeydown(s)}),i&2&&re("aria-haspopup",r.menu?"menu":null)("aria-expanded",r.menuOpen)("aria-controls",r.menuOpen?r.menu?.panelId:null)},inputs:{_deprecatedMatMenuTriggerFor:[0,"mat-menu-trigger-for","_deprecatedMatMenuTriggerFor"],menu:[0,"matMenuTriggerFor","menu"],menuData:[0,"matMenuTriggerData","menuData"],restoreFocus:[0,"matMenuTriggerRestoreFocus","restoreFocus"]},outputs:{menuOpened:"menuOpened",onMenuOpen:"onMenuOpen",menuClosed:"menuClosed",onMenuClose:"onMenuClose"},exportAs:["matMenuTrigger"],features:[Te]})}return t})();var WE=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({imports:[lr,an,ve,Si]})}return t})();var Do=class{_multiple;_emitChanges;compareWith;_selection=new Set;_deselectedToEmit=[];_selectedToEmit=[];_selected=null;get selected(){return this._selected||(this._selected=Array.from(this._selection.values())),this._selected}changed=new x;bulk={select:n=>this._select(n),deselect:n=>this._deselect(n),setSelection:n=>this._setSelection(n)};constructor(n=!1,e,i=!0,r){this._multiple=n,this._emitChanges=i,this.compareWith=r,e&&e.length&&(n?e.forEach(o=>this._markSelected(o)):this._markSelected(e[0]),this._selectedToEmit.length=0)}select(...n){return this._select(n)}deselect(...n){return this._deselect(n)}setSelection(...n){return this._setSelection(n)}toggle(n){return this.isSelected(n)?this.deselect(n):this.select(n)}clear(n=!0){this._unmarkAll();let e=this._hasQueuedChanges();return n&&this._emitChangeEvent(),e}isSelected(n){return this._selection.has(this._getConcreteValue(n))}isEmpty(){return this._selection.size===0}hasValue(){return!this.isEmpty()}sort(n){this._multiple&&this.selected&&this._selected.sort(n)}isMultipleSelection(){return this._multiple}_select(n){this._verifyValueAssignment(n),n.forEach(i=>this._markSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}_deselect(n){this._verifyValueAssignment(n),n.forEach(i=>this._unmarkSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}_setSelection(n){this._verifyValueAssignment(n);let e=this.selected,i=new Set(n.map(o=>this._getConcreteValue(o)));n.forEach(o=>this._markSelected(o)),e.filter(o=>!i.has(this._getConcreteValue(o,i))).forEach(o=>this._unmarkSelected(o));let r=this._hasQueuedChanges();return this._emitChangeEvent(),r}_emitChangeEvent(){this._selected=null,(this._selectedToEmit.length||this._deselectedToEmit.length)&&(this.changed.next({source:this,added:this._selectedToEmit,removed:this._deselectedToEmit}),this._deselectedToEmit=[],this._selectedToEmit=[])}_markSelected(n){n=this._getConcreteValue(n),this.isSelected(n)||(this._multiple||this._unmarkAll(),this.isSelected(n)||this._selection.add(n),this._emitChanges&&this._selectedToEmit.push(n))}_unmarkSelected(n){n=this._getConcreteValue(n),this.isSelected(n)&&(this._selection.delete(n),this._emitChanges&&this._deselectedToEmit.push(n))}_unmarkAll(){this.isEmpty()||this._selection.forEach(n=>this._unmarkSelected(n))}_verifyValueAssignment(n){n.length>1&&this._multiple}_hasQueuedChanges(){return!!(this._deselectedToEmit.length||this._selectedToEmit.length)}_getConcreteValue(n,e){if(this.compareWith){e=e??this._selection;for(let i of e)if(this.compareWith(n,i))return i;return n}else return n}};var JE=(()=>{class t{_renderer;_elementRef;onChange=e=>{};onTouched=()=>{};constructor(e,i){this._renderer=e,this._elementRef=i}setProperty(e,i){this._renderer.setProperty(this._elementRef.nativeElement,e,i)}registerOnTouched(e){this.onTouched=e}registerOnChange(e){this.onChange=e}setDisabledState(e){this.setProperty("disabled",e)}static \u0275fac=function(i){return new(i||t)(ft(Re),ft(O))};static \u0275dir=z({type:t})}return t})(),nF=(()=>{class t extends JE{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ht(t)))(r||t)}})();static \u0275dir=z({type:t,features:[Te]})}return t})(),Gl=new b("");var iF={provide:Gl,useExisting:Pt(()=>ex),multi:!0};function rF(){let t=nn()?nn().getUserAgent():"";return/android (\d+)/.test(t.toLowerCase())}var oF=new b(""),ex=(()=>{class t extends JE{_compositionMode;_composing=!1;constructor(e,i,r){super(e,i),this._compositionMode=r,this._compositionMode==null&&(this._compositionMode=!rF())}writeValue(e){let i=e??"";this.setProperty("value",i)}_handleInput(e){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(e)}_compositionStart(){this._composing=!0}_compositionEnd(e){this._composing=!1,this._compositionMode&&this.onChange(e)}static \u0275fac=function(i){return new(i||t)(ft(Re),ft(O),ft(oF,8))};static \u0275dir=z({type:t,selectors:[["input","formControlName","",3,"type","checkbox",3,"ngNoCva",""],["textarea","formControlName","",3,"ngNoCva",""],["input","formControl","",3,"type","checkbox",3,"ngNoCva",""],["textarea","formControl","",3,"ngNoCva",""],["input","ngModel","",3,"type","checkbox",3,"ngNoCva",""],["textarea","ngModel","",3,"ngNoCva",""],["","ngDefaultControl",""]],hostBindings:function(i,r){i&1&&D("input",function(s){return r._handleInput(s.target.value)})("blur",function(){return r.onTouched()})("compositionstart",function(){return r._compositionStart()})("compositionend",function(s){return r._compositionEnd(s.target.value)})},standalone:!1,features:[Be([iF]),Te]})}return t})();function Rv(t){return t==null||Ov(t)===0}function Ov(t){return t==null?null:Array.isArray(t)||typeof t=="string"?t.length:t instanceof Set?t.size:null}var Wl=new b(""),tx=new b(""),sF=/^(?=.{1,254}$)(?=.{1,64}@)[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/,Eo=class{static min(n){return aF(n)}static max(n){return lF(n)}static required(n){return nx(n)}static requiredTrue(n){return cF(n)}static email(n){return dF(n)}static minLength(n){return uF(n)}static maxLength(n){return fF(n)}static pattern(n){return hF(n)}static nullValidator(n){return vf()}static compose(n){return lx(n)}static composeAsync(n){return cx(n)}};function aF(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e<t?{min:{min:t,actual:n.value}}:null}}function lF(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e>t?{max:{max:t,actual:n.value}}:null}}function nx(t){return Rv(t.value)?{required:!0}:null}function cF(t){return t.value===!0?null:{required:!0}}function dF(t){return Rv(t.value)||sF.test(t.value)?null:{email:!0}}function uF(t){return n=>{let e=n.value?.length??Ov(n.value);return e===null||e===0?null:e<t?{minlength:{requiredLength:t,actualLength:e}}:null}}function fF(t){return n=>{let e=n.value?.length??Ov(n.value);return e!==null&&e>t?{maxlength:{requiredLength:t,actualLength:e}}:null}}function hF(t){if(!t)return vf;let n,e;return typeof t=="string"?(e="",t.charAt(0)!=="^"&&(e+="^"),e+=t,t.charAt(t.length-1)!=="$"&&(e+="$"),n=new RegExp(e)):(e=t.toString(),n=t),i=>{if(Rv(i.value))return null;let r=i.value;return n.test(r)?null:{pattern:{requiredPattern:e,actualValue:r}}}}function vf(t){return null}function ix(t){return t!=null}function rx(t){return di(t)?Ve(t):t}function ox(t){let n={};return t.forEach(e=>{n=e!=null?v(v({},n),e):n}),Object.keys(n).length===0?null:n}function sx(t,n){return n.map(e=>e(t))}function mF(t){return!t.validate}function ax(t){return t.map(n=>mF(n)?n:e=>n.validate(e))}function lx(t){if(!t)return null;let n=t.filter(ix);return n.length==0?null:function(e){return ox(sx(e,n))}}function Pv(t){return t!=null?lx(ax(t)):null}function cx(t){if(!t)return null;let n=t.filter(ix);return n.length==0?null:function(e){let i=sx(e,n).map(rx);return ra(i).pipe(oe(ox))}}function Fv(t){return t!=null?cx(ax(t)):null}function qE(t,n){return t===null?[n]:Array.isArray(t)?[...t,n]:[t,n]}function dx(t){return t._rawValidators}function ux(t){return t._rawAsyncValidators}function kv(t){return t?Array.isArray(t)?t:[t]:[]}function bf(t,n){return Array.isArray(t)?t.includes(n):t===n}function YE(t,n){let e=kv(n);return kv(t).forEach(r=>{bf(e,r)||e.push(r)}),e}function ZE(t,n){return kv(n).filter(e=>!bf(t,e))}var _f=class{get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators=[];_rawAsyncValidators=[];_setValidators(n){this._rawValidators=n||[],this._composedValidatorFn=Pv(this._rawValidators)}_setAsyncValidators(n){this._rawAsyncValidators=n||[],this._composedAsyncValidatorFn=Fv(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_onDestroyCallbacks=[];_registerOnDestroy(n){this._onDestroyCallbacks.push(n)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(n=>n()),this._onDestroyCallbacks=[]}reset(n=void 0){this.control?.reset(n)}hasError(n,e){return this.control?this.control.hasError(n,e):!1}getError(n,e){return this.control?this.control.getError(n,e):null}},Bs=class extends _f{name;get formDirective(){return null}get path(){return null}};var jl="VALID",gf="INVALID",Ls="PENDING",Hl="DISABLED",hr=class{},yf=class extends hr{value;source;constructor(n,e){super(),this.value=n,this.source=e}},zl=class extends hr{pristine;source;constructor(n,e){super(),this.pristine=n,this.source=e}},$l=class extends hr{touched;source;constructor(n,e){super(),this.touched=n,this.source=e}},Vs=class extends hr{status;source;constructor(n,e){super(),this.status=n,this.source=e}},Sf=class extends hr{source;constructor(n){super(),this.source=n}},js=class extends hr{source;constructor(n){super(),this.source=n}};function fx(t){return(wf(t)?t.validators:t)||null}function pF(t){return Array.isArray(t)?Pv(t):t||null}function hx(t,n){return(wf(n)?n.asyncValidators:t)||null}function gF(t){return Array.isArray(t)?Fv(t):t||null}function wf(t){return t!=null&&!Array.isArray(t)&&typeof t=="object"}function vF(t,n,e){let i=t.controls;if(!(n?Object.keys(i):i).length)throw new w(1e3,"");if(!mx(i,e))throw new w(1001,"")}function bF(t,n,e){t._forEachChild((i,r)=>{if(e[r]===void 0)throw new w(-1002,"")})}var Cf=class{_pendingDirty=!1;_hasOwnPendingAsyncValidator=null;_pendingTouched=!1;_onCollectionChange=()=>{};_updateOn;_hasRequired=U(!1);_parent=null;_asyncValidationSubscription;_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators;_rawAsyncValidators;value;constructor(n,e){this._assignValidators(n),this._assignAsyncValidators(e)}get validator(){return this._composedValidatorFn}set validator(n){this._rawValidators=this._composedValidatorFn=n,this._updateHasRequiredValidator()}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(n){this._rawAsyncValidators=this._composedAsyncValidatorFn=n}get parent(){return this._parent}get status(){return Ne(this.statusReactive)}set status(n){Ne(()=>this.statusReactive.set(n))}_status=K(()=>this.statusReactive());statusReactive=U(void 0);get valid(){return this.status===jl}get invalid(){return this.status===gf}get pending(){return this.status===Ls}get disabled(){return this.status===Hl}get enabled(){return this.status!==Hl}errors;get pristine(){return Ne(this.pristineReactive)}set pristine(n){Ne(()=>this.pristineReactive.set(n))}_pristine=K(()=>this.pristineReactive());pristineReactive=U(!0);get dirty(){return!this.pristine}get touched(){return Ne(this.touchedReactive)}set touched(n){Ne(()=>this.touchedReactive.set(n))}_touched=K(()=>this.touchedReactive());touchedReactive=U(!1);get untouched(){return!this.touched}_events=new x;events=this._events.asObservable();valueChanges;statusChanges;get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(n){this._assignValidators(n)}setAsyncValidators(n){this._assignAsyncValidators(n)}addValidators(n){this.setValidators(YE(n,this._rawValidators))}addAsyncValidators(n){this.setAsyncValidators(YE(n,this._rawAsyncValidators))}removeValidators(n){this.setValidators(ZE(n,this._rawValidators))}removeAsyncValidators(n){this.setAsyncValidators(ZE(n,this._rawAsyncValidators))}hasValidator(n){return bf(this._rawValidators,n)}hasAsyncValidator(n){return bf(this._rawAsyncValidators,n)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(n={}){let e=this.touched===!1;this.touched=!0;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsTouched(N(v({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new $l(!0,i))}markAllAsDirty(n={}){this.markAsDirty({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsDirty(n))}markAllAsTouched(n={}){this.markAsTouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsTouched(n))}markAsUntouched(n={}){let e=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsUntouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:i})}),n.onlySelf||this._parent?._updateTouched(n,i),e&&n.emitEvent!==!1&&this._events.next(new $l(!1,i))}markAsDirty(n={}){let e=this.pristine===!0;this.pristine=!1;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsDirty(N(v({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new zl(!1,i))}markAsPristine(n={}){let e=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsPristine({onlySelf:!0,emitEvent:n.emitEvent})}),n.onlySelf||this._parent?._updatePristine(n,i),e&&n.emitEvent!==!1&&this._events.next(new zl(!0,i))}markAsPending(n={}){this.status=Ls;let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Vs(this.status,e)),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.markAsPending(N(v({},n),{sourceControl:e}))}disable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=Hl,this.errors=null,this._forEachChild(r=>{r.disable(N(v({},n),{onlySelf:!0}))}),this._updateValue();let i=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new yf(this.value,i)),this._events.next(new Vs(this.status,i)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(N(v({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(r=>r(!0))}enable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=jl,this._forEachChild(i=>{i.enable(N(v({},n),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent}),this._updateAncestors(N(v({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(i=>i(!1))}_updateAncestors(n,e){n.onlySelf||(this._parent?.updateValueAndValidity(n),n.skipPristineCheck||this._parent?._updatePristine({},e),this._parent?._updateTouched({},e))}setParent(n){this._parent=n}getRawValue(){return this.value}updateValueAndValidity(n={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let i=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===jl||this.status===Ls)&&this._runAsyncValidator(i,n.emitEvent)}let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new yf(this.value,e)),this._events.next(new Vs(this.status,e)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.updateValueAndValidity(N(v({},n),{sourceControl:e}))}_updateTreeValidity(n={emitEvent:!0}){this._forEachChild(e=>e._updateTreeValidity(n)),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?Hl:jl}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(n,e){if(this.asyncValidator){this.status=Ls,this._hasOwnPendingAsyncValidator={emitEvent:e!==!1,shouldHaveEmitted:n!==!1};let i=rx(this.asyncValidator(this));this._asyncValidationSubscription=i.subscribe(r=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(r,{emitEvent:e,shouldHaveEmitted:n})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let n=(this._hasOwnPendingAsyncValidator?.emitEvent||this._hasOwnPendingAsyncValidator?.shouldHaveEmitted)??!1;return this._hasOwnPendingAsyncValidator=null,n}return!1}setErrors(n,e={}){this.errors=n,this._updateControlsErrors(e.emitEvent!==!1,this,e.shouldHaveEmitted)}get(n){let e=n;return e==null||(Array.isArray(e)||(e=e.split(".")),e.length===0)?null:e.reduce((i,r)=>i&&i._find(r),this)}getError(n,e){let i=e?this.get(e):this;return i?.errors?i.errors[n]:null}hasError(n,e){return!!this.getError(n,e)}get root(){let n=this;for(;n._parent;)n=n._parent;return n}_updateControlsErrors(n,e,i){this.status=this._calculateStatus(),n&&this.statusChanges.emit(this.status),(n||i)&&this._events.next(new Vs(this.status,e)),this._parent&&this._parent._updateControlsErrors(n,e,i)}_initObservables(){this.valueChanges=new W,this.statusChanges=new W}_calculateStatus(){return this._allControlsDisabled()?Hl:this.errors?gf:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(Ls)?Ls:this._anyControlsHaveStatus(gf)?gf:jl}_anyControlsHaveStatus(n){return this._anyControls(e=>e.status===n)}_anyControlsDirty(){return this._anyControls(n=>n.dirty)}_anyControlsTouched(){return this._anyControls(n=>n.touched)}_updatePristine(n,e){let i=!this._anyControlsDirty(),r=this.pristine!==i;this.pristine=i,n.onlySelf||this._parent?._updatePristine(n,e),r&&this._events.next(new zl(this.pristine,e))}_updateTouched(n={},e){this.touched=this._anyControlsTouched(),this._events.next(new $l(this.touched,e)),n.onlySelf||this._parent?._updateTouched(n,e)}_onDisabledChange=[];_registerOnCollectionChange(n){this._onCollectionChange=n}_setUpdateStrategy(n){wf(n)&&n.updateOn!=null&&(this._updateOn=n.updateOn)}_parentMarkedDirty(n){return!n&&!!this._parent?.dirty&&!this._parent._anyControlsDirty()}_find(n){return null}_assignValidators(n){this._rawValidators=Array.isArray(n)?n.slice():n,this._composedValidatorFn=pF(this._rawValidators),this._updateHasRequiredValidator()}_assignAsyncValidators(n){this._rawAsyncValidators=Array.isArray(n)?n.slice():n,this._composedAsyncValidatorFn=gF(this._rawAsyncValidators)}_updateHasRequiredValidator(){Ne(()=>this._hasRequired.set(this.hasValidator(Eo.required)))}};function mx(t,n){return Object.hasOwn(t,n)}function _F(t){return t.tagName==="INPUT"||t.tagName==="SELECT"||t.tagName==="TEXTAREA"}function yF(t,n,e,i){switch(e){case"name":t.setAttribute(n,e,i);break;case"disabled":case"readonly":case"required":i?t.setAttribute(n,e,""):t.removeAttribute(n,e);break;case"max":case"min":case"minLength":case"maxLength":i!==void 0?t.setAttribute(n,e,i.toString()):t.removeAttribute(n,e);break}}var Av=class{kind;context;control;message;constructor({kind:n,context:e,control:i}){this.kind=n,this.context=e,this.control=i}};var SF=(()=>{class t{_validator=vf;_onChange;_enabled;ngOnChanges(e){if(this.inputName in e){let i=this.normalizeInput(e[this.inputName].currentValue);this._enabled=this.enabled(i),this._validator=this._enabled?this.createValidator(i):vf,this._onChange?.()}}validate(e){return this._validator(e)}registerOnValidatorChange(e){this._onChange=e}enabled(e){return e!=null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=z({type:t,features:[at]})}return t})();var CF={provide:Wl,useExisting:Pt(()=>px),multi:!0};var px=(()=>{class t extends SF{required;inputName="required";normalizeInput=V;createValidator=e=>nx;enabled(e){return e}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ht(t)))(r||t)}})();static \u0275dir=z({type:t,selectors:[["","required","","formControlName","",3,"type","checkbox"],["","required","","formControl","",3,"type","checkbox"],["","required","","ngModel","",3,"type","checkbox"]],hostVars:1,hostBindings:function(i,r){i&2&&re("required",r._enabled?"":null)},inputs:{required:"required"},standalone:!1,features:[Be([CF]),Te]})}return t})();var gx=new b("",{factory:()=>DF}),DF="always";function XE(t,n,e=!0){let i=()=>{};n?.valueAccessor?.registerOnChange(i),n?.valueAccessor?.registerOnTouched(i),Ef(t,n),t&&(n._invokeOnDestroyCallbacks(),t._registerOnCollectionChange(()=>{}))}function Df(t,n){t.forEach(e=>{e.registerOnValidatorChange&&e.registerOnValidatorChange(n)})}function vx(t,n){let e=dx(t);n.validator!==null?t.setValidators(qE(e,n.validator)):typeof e=="function"&&t.setValidators([e]);let i=ux(t);n.asyncValidator!==null?t.setAsyncValidators(qE(i,n.asyncValidator)):typeof i=="function"&&t.setAsyncValidators([i]);let r=()=>t.updateValueAndValidity();Df(n._rawValidators,r),Df(n._rawAsyncValidators,r)}function Ef(t,n){let e=!1;if(t!==null){if(n.validator!==null){let r=dx(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(s=>s!==n.validator);o.length!==r.length&&(e=!0,t.setValidators(o))}}if(n.asyncValidator!==null){let r=ux(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(s=>s!==n.asyncValidator);o.length!==r.length&&(e=!0,t.setAsyncValidators(o))}}}let i=()=>{};return Df(n._rawValidators,i),Df(n._rawAsyncValidators,i),e}function bx(t,n){t==null,vx(t,n)}function EF(t,n){return Ef(t,n)}function xF(t){return Object.getPrototypeOf(t.constructor)===nF}function _x(t,n){t._syncPendingControls(),n.forEach(e=>{let i=e.control;i.updateOn==="submit"&&i._pendingChange&&(e.viewToModelUpdate(i._pendingValue),i._pendingChange=!1)})}function wF(t,n){if(!n)return null;Array.isArray(n);let e,i,r;return n.forEach(o=>{o.constructor===ex?e=o:xF(o)?i=o:r=o}),r||i||e||null}function IF(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}var Hs=class extends _f{_parent=null;name=null;valueAccessor=null;isCustomControlBased=!1;userOnReset;resetSubscription;set onReset(n){this.userOnReset=n,this.resetSubscription?.unsubscribe(),this.resetSubscription=void 0,this.control&&(this.resetSubscription=this.control.events.subscribe(e=>{e instanceof js&&this.control&&this.userOnReset?.(this.control.value)}),this.subscription?.add(this.resetSubscription))}isNativeFormElement=!1;rawValueAccessors;_selectedValueAccessor=null;get selectedValueAccessor(){return this._selectedValueAccessor??=wF(this,this.rawValueAccessors)}parseErrorsValidator=null;renderer;injector;requiredValidatorViaDi;subscription;customControlBindings=null;constructor(n,e,i){super(),this.injector=n,this.renderer=e,this.rawValueAccessors=i,this.injector?.get(qe)?.onDestroy(()=>{this.removeParseErrorsValidator(this.control),this.subscription?.unsubscribe()})}setupCustomControl(){this.subscription?.unsubscribe();let n=this.injector?.get(Oe);if(!this.control||!n)return;let e=n.markForCheck.bind(n);this.subscription=new pe,this.subscription.add(this.control.valueChanges.subscribe(e)),this.subscription.add(this.control.statusChanges.subscribe(e)),this.resetSubscription?.unsubscribe(),this.resetSubscription=void 0,this.userOnReset&&(this.resetSubscription=this.control.events.subscribe(i=>{i instanceof js&&this.control&&this.userOnReset?.(this.control.value)}),this.subscription.add(this.resetSubscription)),this.parseErrorsValidator&&this.control.addValidators(this.parseErrorsValidator)}ngControlCreate(n){!n.nativeElement.hasAttribute?.("ngNoCva")&&(this.rawValueAccessors&&this.rawValueAccessors.length>0||this.valueAccessor!==null)||!n.customControl||(this.isCustomControlBased=!0,n.listenToCustomControlModel(r=>{this.control?.setValue(r,{emitModelToViewChange:!1}),this.control?.markAsDirty(),this.viewToModelUpdate(r)}),n.listenToCustomControlOutput("touch",()=>{this.control?.markAsTouched()}),this.customControlBindings={},this.isNativeFormElement=_F(n.nativeElement),this.requiredValidatorViaDi=this._rawValidators.find(r=>r instanceof px))}ngControlUpdate(n,e){if(!this.isCustomControlBased)return;let i=this.control,r=this.customControlBindings;Object.is(r.value,i.value)||(r.value=i.value,n.setCustomControlModelInput(i.value)),this.bindControlProperty(n,r,"touched",i.touched),this.bindControlProperty(n,r,"dirty",i.dirty),this.bindControlProperty(n,r,"valid",i.valid),this.bindControlProperty(n,r,"invalid",i.invalid),this.bindControlProperty(n,r,"pending",i.pending),this.bindControlProperty(n,r,"disabled",i.disabled),this.shouldBindRequired&&this.bindControlProperty(n,r,"required",this.isRequired);let o=i.errors;if(r.errors!==o){r.errors=o;let s=this._convertErrors(o);n.setInputOnDirectives("errors",s)}}get isRequired(){return(this.requiredValidatorViaDi?._enabled||this.control?._hasRequired())??!1}get shouldBindRequired(){return!0}bindControlProperty(n,e,i,r){if(e[i]===r)return;e[i]=r;let o=n.setInputOnDirectives(i,r);this.isNativeFormElement&&!o&&(i==="disabled"||i==="required")&&this.renderer&&yF(this.renderer,n.nativeElement,i,r)}_convertErrors(n){if(n===null)return[];let e=this.control;return Object.entries(n).map(([i,r])=>new Av({context:r,kind:i,control:e}))}setParseErrorSource(n){if(n===void 0)return;let e=null,i=K(()=>{let r=n();return r.length===0?null:r.reduce((o,s)=>(o[s.kind]=s,o),{})});this.parseErrorsValidator=(()=>e).bind(this),ut(()=>{e=i(),this.control?.updateValueAndValidity({emitEvent:!1})},{injector:this.injector})}removeParseErrorsValidator(n){this.parseErrorsValidator&&(n?.removeValidators(this.parseErrorsValidator),n?.updateValueAndValidity({emitEvent:!1}))}};var xf=class extends Cf{constructor(n,e,i){super(fx(e),hx(i,e)),this.controls=n,this._initObservables(),this._setUpdateStrategy(e),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;registerControl(n,e){let i=this._find(n);return i||(this.controls[n]=e,e.setParent(this),e._registerOnCollectionChange(this._onCollectionChange),e)}addControl(n,e,i={}){this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}removeControl(n,e={}){let i=this._find(n);i&&i._registerOnCollectionChange(()=>{}),delete this.controls[n],this.updateValueAndValidity({emitEvent:e.emitEvent}),this._onCollectionChange()}setControl(n,e,i={}){let r=this._find(n);r&&r._registerOnCollectionChange(()=>{}),delete this.controls[n],e&&this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}contains(n){return this._find(n)?.enabled===!0}setValue(n,e={}){Ne(()=>{bF(this,!0,n),Object.keys(n).forEach(i=>{vF(this,!0,i),this.controls[i].setValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e)})}patchValue(n,e={}){n!=null&&(Object.keys(n).forEach(i=>{let r=this._find(i);r&&r.patchValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e))}reset(n={},e={}){this._forEachChild((i,r)=>{i.reset(n?n[r]:null,N(v({},e),{onlySelf:!0}))}),this._updatePristine(e,this),this._updateTouched(e,this),this.updateValueAndValidity(e),e?.emitEvent!==!1&&this._events.next(new js(this))}getRawValue(){return this._reduceChildren({},(n,e,i)=>(n[i]=e.getRawValue(),n))}_syncPendingControls(){let n=this._reduceChildren(!1,(e,i)=>i._syncPendingControls()?!0:e);return n&&this.updateValueAndValidity({onlySelf:!0}),n}_forEachChild(n){Object.keys(this.controls).forEach(e=>{let i=this.controls[e];i&&n(i,e)})}_setUpControls(){this._forEachChild(n=>{n.setParent(this),n._registerOnCollectionChange(this._onCollectionChange)})}_updateValue(){this.value=this._reduceValue()}_anyControls(n){for(let[e,i]of Object.entries(this.controls))if(this.contains(e)&&n(i))return!0;return!1}_reduceValue(){let n={};return this._reduceChildren(n,(e,i,r)=>((i.enabled||this.disabled)&&(e[r]=i.value),e))}_reduceChildren(n,e){let i=n;return this._forEachChild((r,o)=>{i=e(i,r,o)}),i}_allControlsDisabled(){for(let n of Object.keys(this.controls))if(this.controls[n].enabled)return!1;return Object.keys(this.controls).length>0||this.disabled}_find(n){return mx(this.controls,n)?this.controls[n]:null}};var MF={provide:Bs,useExisting:Pt(()=>ql)},Ul=Promise.resolve(),ql=(()=>{class t extends Bs{callSetDisabledState;get submitted(){return Ne(this.submittedReactive)}_submitted=K(()=>this.submittedReactive());submittedReactive=U(!1);_directives=new Set;form;ngSubmit=new W;options;constructor(e,i,r){super(),this.callSetDisabledState=r,this.form=new xf({},Pv(e),Fv(i))}ngAfterViewInit(){this._setUpdateStrategy()}get formDirective(){return this}get control(){return this.form}get path(){return[]}get controls(){return this.form.controls}addControl(e){Ul.then(()=>{let i=this._findContainer(e.path);e.control=i.registerControl(e.name,e.control),e._setupWithForm(this.callSetDisabledState),e.control.updateValueAndValidity({emitEvent:!1}),this._directives.add(e)})}getControl(e){return this.form.get(e.path)}removeControl(e){Ul.then(()=>{this._findContainer(e.path)?.removeControl(e.name),this._directives.delete(e)})}addFormGroup(e){Ul.then(()=>{let i=this._findContainer(e.path),r=new xf({});bx(r,e),i.registerControl(e.name,r),r.updateValueAndValidity({emitEvent:!1})})}removeFormGroup(e){Ul.then(()=>{this._findContainer(e.path)?.removeControl?.(e.name)})}getFormGroup(e){return this.form.get(e.path)}updateModel(e,i){Ul.then(()=>{this.form.get(e.path).setValue(i)})}setValue(e){this.control.setValue(e)}onSubmit(e){return this.submittedReactive.set(!0),_x(this.form,this._directives),this.ngSubmit.emit(e),this.form._events.next(new Sf(this.control)),e?.target?.method==="dialog"}onReset(){this.resetForm()}resetForm(e=void 0){this.form.reset(e),this.submittedReactive.set(!1)}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.form._updateOn=this.options.updateOn)}_findContainer(e){return e.pop(),e.length?this.form.get(e):this.form}static \u0275fac=function(i){return new(i||t)(ft(Wl,10),ft(tx,10),ft(gx,8))};static \u0275dir=z({type:t,selectors:[["form",3,"ngNoForm","",3,"formGroup","",3,"formArray",""],["ng-form"],["","ngForm",""]],hostBindings:function(i,r){i&1&&D("submit",function(s){return r.onSubmit(s)})("reset",function(){return r.onReset()})},inputs:{options:[0,"ngFormOptions","options"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[Be([MF]),Te]})}return t})();function KE(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function QE(t){return typeof t=="object"&&t!==null&&Object.keys(t).length===2&&"value"in t&&"disabled"in t}var TF=class extends Cf{defaultValue=null;_onChange=[];_pendingValue;_pendingChange=!1;constructor(n=null,e,i){super(fx(e),hx(i,e)),this._applyFormState(n),this._setUpdateStrategy(e),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),wf(e)&&(e.nonNullable||e.initialValueIsDefault)&&(QE(n)?this.defaultValue=n.value:this.defaultValue=n)}setValue(n,e={}){Ne(()=>{this.value=this._pendingValue=n,this._onChange.length&&e.emitModelToViewChange!==!1&&this._onChange.forEach(i=>i(this.value,e.emitViewToModelChange!==!1)),this.updateValueAndValidity(e)})}patchValue(n,e={}){this.setValue(n,e)}reset(n=this.defaultValue,e={}){this._applyFormState(n),this.markAsPristine(e),this.markAsUntouched(e),this.setValue(this.value,e),e.overwriteDefaultValue&&(this.defaultValue=this.value),this._pendingChange=!1,e?.emitEvent!==!1&&this._events.next(new js(this))}_updateValue(){}_anyControls(n){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(n){this._onChange.push(n)}_unregisterOnChange(n){KE(this._onChange,n)}registerOnDisabledChange(n){this._onDisabledChange.push(n)}_unregisterOnDisabledChange(n){KE(this._onDisabledChange,n)}_forEachChild(n){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(n){QE(n)?(this.value=this._pendingValue=n.value,n.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=n}};var NF=t=>t instanceof TF;var kF=(()=>{class t extends Bs{callSetDisabledState;get submitted(){return Ne(this._submittedReactive)}set submitted(e){this._submittedReactive.set(e)}_submitted=K(()=>this._submittedReactive());_submittedReactive=U(!1);_oldForm;_onCollectionChange=()=>this._updateDomValue();directives=[];constructor(e,i,r){super(),this.callSetDisabledState=r,this._setValidators(e),this._setAsyncValidators(i)}ngOnChanges(e){this.onChanges(e)}ngOnDestroy(){this.onDestroy()}onChanges(e){this._checkFormPresent(),Object.hasOwn(e,"form")&&(this._updateValidators(),this._updateDomValue(),this._updateRegistrations(),this._oldForm=this.form)}onDestroy(){this.form&&(Ef(this.form,this),this.form._onCollectionChange===this._onCollectionChange&&this.form._registerOnCollectionChange(()=>{}))}get formDirective(){return this}get path(){return[]}addControl(e){let i=this.form.get(e.path);return e._setupWithForm(i,this.callSetDisabledState),i.updateValueAndValidity({emitEvent:!1}),this.directives.push(e),i}getControl(e){return this.form.get(e.path)}removeControl(e){XE(e.control||null,e,!1),IF(this.directives,e)}addFormGroup(e){this._setUpFormContainer(e)}removeFormGroup(e){this._cleanUpFormContainer(e)}getFormGroup(e){return this.form.get(e.path)}getFormArray(e){return this.form.get(e.path)}addFormArray(e){this._setUpFormContainer(e)}removeFormArray(e){this._cleanUpFormContainer(e)}updateModel(e,i){this.form.get(e.path).setValue(i)}onReset(){this.resetForm()}resetForm(e=void 0,i={}){this.form.reset(e,i),this._submittedReactive.set(!1)}onSubmit(e){return this.submitted=!0,_x(this.form,this.directives),this.ngSubmit.emit(e),this.form._events.next(new Sf(this.control)),e?.target?.method==="dialog"}_updateDomValue(){this.directives.forEach(e=>{let i=e.control,r=this.form.get(e.path);i!==r&&(XE(i||null,e),NF(r)&&e._setupWithForm(r,this.callSetDisabledState))}),this.form._updateTreeValidity({emitEvent:!1})}_setUpFormContainer(e){let i=this.form.get(e.path);bx(i,e),i.updateValueAndValidity({emitEvent:!1})}_cleanUpFormContainer(e){let i=this.form?.get(e.path);i&&EF(i,e)&&i.updateValueAndValidity({emitEvent:!1})}_updateRegistrations(){this.form._registerOnCollectionChange(this._onCollectionChange),this._oldForm?._registerOnCollectionChange(()=>{})}_updateValidators(){vx(this.form,this),this._oldForm&&Ef(this._oldForm,this)}_checkFormPresent(){this.form}static \u0275fac=function(i){return new(i||t)(ft(Wl,10),ft(tx,10),ft(gx,8))};static \u0275dir=z({type:t,features:[Te,at]})}return t})(),AF={provide:Bs,useExisting:Pt(()=>Yl)},Yl=(()=>{class t extends kF{form=null;ngSubmit=new W;get control(){return this.form}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ht(t)))(r||t)}})();static \u0275dir=z({type:t,selectors:[["","formGroup",""]],hostBindings:function(i,r){i&1&&D("submit",function(s){return r.onSubmit(s)})("reset",function(){return r.onReset()})},inputs:{form:[0,"formGroup","form"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[Be([AF]),Te]})}return t})();var If=new b("");var Mf=(()=>{class t{_animationsDisabled=Ee();state="unchecked";disabled=!1;appearance="full";static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-pseudo-checkbox"]],hostAttrs:[1,"mat-pseudo-checkbox"],hostVars:12,hostBindings:function(i,r){i&2&&B("mat-pseudo-checkbox-indeterminate",r.state==="indeterminate")("mat-pseudo-checkbox-checked",r.state==="checked")("mat-pseudo-checkbox-disabled",r.disabled)("mat-pseudo-checkbox-minimal",r.appearance==="minimal")("mat-pseudo-checkbox-full",r.appearance==="full")("_mat-animation-noopable",r._animationsDisabled)},inputs:{state:"state",disabled:"disabled",appearance:"appearance"},decls:0,vars:0,template:function(i,r){},styles:[`.mat-pseudo-checkbox {
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
`],encapsulation:2})}return t})();var RF=["text"],OF=[[["mat-icon"]],"*"],PF=["mat-icon","*"];function FF(t,n){if(t&1&&ie(0,"mat-pseudo-checkbox",1),t&2){let e=C();L("disabled",e.disabled)("state",e.selected?"checked":"unchecked")}}function LF(t,n){if(t&1&&ie(0,"mat-pseudo-checkbox",3),t&2){let e=C();L("disabled",e.disabled)}}function VF(t,n){if(t&1&&(m(0,"span",4),y(1),g()),t&2){let e=C();_(),At("(",e.group.label,")")}}var Vv=new b("MAT_OPTION_PARENT_COMPONENT"),Bv=new b("MatOptgroup");var Lv=class{source;isUserInput;constructor(n,e=!1){this.source=n,this.isUserInput=e}},mr=(()=>{class t{_element=u(O);_changeDetectorRef=u(Oe);_parent=u(Vv,{optional:!0});group=u(Bv,{optional:!0});_signalDisableRipple=!1;_selected=!1;_active=!1;_mostRecentViewValue="";get multiple(){return this._parent&&this._parent.multiple}get selected(){return this._selected}value;id=u(Ie).getId("mat-option-");get disabled(){return this.group&&this.group.disabled||this._disabled()}set disabled(e){this._disabled.set(e)}_disabled=U(!1);get disableRipple(){return this._signalDisableRipple?this._parent.disableRipple():!!this._parent?.disableRipple}get hideSingleSelectionIndicator(){return!!(this._parent&&this._parent.hideSingleSelectionIndicator)}onSelectionChange=new W;_text;_stateChanges=new x;constructor(){let e=u(ze);e.load(Zn),e.load(Ds),this._signalDisableRipple=!!this._parent&&Kt(this._parent.disableRipple)}get active(){return this._active}get viewValue(){return(this._text?.nativeElement.textContent||"").trim()}select(e=!0){this._selected||(this._selected=!0,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}deselect(e=!0){this._selected&&(this._selected=!1,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}focus(e,i){let r=this._getHostElement();typeof r.focus=="function"&&r.focus(i)}setActiveStyles(){this._active||(this._active=!0,this._changeDetectorRef.markForCheck())}setInactiveStyles(){this._active&&(this._active=!1,this._changeDetectorRef.markForCheck())}getLabel(){return this.viewValue}_handleKeydown(e){(e.keyCode===13||e.keyCode===32)&&!$e(e)&&(this._selectViaInteraction(),e.preventDefault())}_selectViaInteraction(){this.disabled||(this._selected=this.multiple?!this._selected:!0,this._changeDetectorRef.markForCheck(),this._emitSelectionChangeEvent(!0))}_getTabIndex(){return this.disabled?"-1":"0"}_getHostElement(){return this._element.nativeElement}ngAfterViewChecked(){if(this._selected){let e=this.viewValue;e!==this._mostRecentViewValue&&(this._mostRecentViewValue&&this._stateChanges.next(),this._mostRecentViewValue=e)}}ngOnDestroy(){this._stateChanges.complete()}_emitSelectionChangeEvent(e=!1){this.onSelectionChange.emit(new Lv(this,e))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-option"]],viewQuery:function(i,r){if(i&1&&Ue(RF,7),i&2){let o;ce(o=de())&&(r._text=o.first)}},hostAttrs:["role","option",1,"mat-mdc-option","mdc-list-item"],hostVars:11,hostBindings:function(i,r){i&1&&D("click",function(){return r._selectViaInteraction()})("keydown",function(s){return r._handleKeydown(s)}),i&2&&(Et("id",r.id),re("aria-selected",r.selected)("aria-disabled",r.disabled.toString()),B("mdc-list-item--selected",r.selected)("mat-mdc-option-multiple",r.multiple)("mat-mdc-option-active",r.active)("mdc-list-item--disabled",r.disabled))},inputs:{value:"value",id:"id",disabled:[2,"disabled","disabled",V]},outputs:{onSelectionChange:"onSelectionChange"},exportAs:["matOption"],ngContentSelectors:PF,decls:8,vars:5,consts:[["text",""],["aria-hidden","true",1,"mat-mdc-option-pseudo-checkbox",3,"disabled","state"],[1,"mdc-list-item__primary-text"],["state","checked","aria-hidden","true","appearance","minimal",1,"mat-mdc-option-pseudo-checkbox",3,"disabled"],[1,"cdk-visually-hidden"],["aria-hidden","true","mat-ripple","",1,"mat-mdc-option-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled"]],template:function(i,r){i&1&&(Pe(OF),P(0,FF,1,2,"mat-pseudo-checkbox",1),me(1),m(2,"span",2,0),me(4,1),g(),P(5,LF,1,1,"mat-pseudo-checkbox",3),P(6,VF,2,1,"span",4),ie(7,"div",5)),i&2&&(F(r.multiple?0:-1),_(5),F(!r.multiple&&r.selected&&!r.hideSingleSelectionIndicator?5:-1),_(),F(r.group&&r.group._inert?6:-1),_(),L("matRippleTrigger",r._getHostElement())("matRippleDisabled",r.disabled||r.disableRipple))},dependencies:[Mf,ar],styles:[`.mat-mdc-option {
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
`],encapsulation:2})}return t})();function yx(t,n,e){if(e.length){let i=n.toArray(),r=e.toArray(),o=0;for(let s=0;s<t+1;s++)i[s].group&&i[s].group===r[o]&&o++;return o}return 0}function Sx(t,n,e,i){return t<e?t:t+n>e+i?Math.max(0,t-i+n):e}var Tf=(()=>{class t{isErrorState(e,i){return!!(e&&e.invalid&&(e.touched||i&&i.submitted))}isSignalErrorState(e){if(!e)return!1;let i=e().invalid(),r=e().touched();return i&&r}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();var Us=class{_defaultMatcher;_parentFormGroup;_parentForm;_stateChanges;errorState=!1;matcher;ngControl;formField;constructor(n,e,i,r,o){this._defaultMatcher=n,this._parentFormGroup=i,this._parentForm=r,this._stateChanges=o,e?Kt(e.field)&&!e.updateValueAndValidity?(this.formField=e,this.ngControl=null):(this.formField=null,this.ngControl=e):this.ngControl=this.formField=null}updateErrorState(){let n=this.errorState,e=this._getCurrentErrorState(this.matcher||this._defaultMatcher);e!==n&&(this.errorState=e,this._stateChanges.next())}_getCurrentErrorState(n){if(this.formField&&n?.isSignalErrorState)return n.isSignalErrorState(this.formField.field())??!1;let e=this._parentFormGroup||this._parentForm,i=this.ngControl?this.ngControl.control:null;return n?.isErrorState(i,e)??!1}};var Cx=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({imports:[ve]})}return t})();var jv=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({imports:[lr,Cx,mr,ve]})}return t})();var BF=["trigger"],jF=["panel"],HF=[[["mat-select-trigger"]],"*"],UF=["mat-select-trigger","*"];function zF(t,n){if(t&1&&(m(0,"span",4),y(1),g()),t&2){let e=C();_(),ye(e.placeholder)}}function $F(t,n){t&1&&me(0)}function GF(t,n){if(t&1&&(m(0,"span",11),y(1),g()),t&2){let e=C(2);_(),ye(e.triggerValue)}}function WF(t,n){if(t&1&&(m(0,"span",5),P(1,$F,1,0)(2,GF,2,1,"span",11),g()),t&2){let e=C();_(),F(e.customTrigger?1:2)}}function qF(t,n){if(t&1){let e=Le();m(0,"div",12,1),D("keydown",function(r){J(e);let o=C();return ee(o._handleKeydown(r))}),me(2,1),g()}if(t&2){let e=C();xt(e.panelClass),B("mat-select-panel-animations-enabled",!e._animationsDisabled)("mat-primary",e._parentFormField?.color==="primary")("mat-accent",e._parentFormField?.color==="accent")("mat-warn",e._parentFormField?.color==="warn")("mat-undefined",!e._parentFormField?.color),re("id",e.id+"-panel")("aria-multiselectable",e.multiple)("aria-label",e.ariaLabel||null)("aria-labelledby",e._getPanelAriaLabelledby())}}var YF=new b("mat-select-scroll-strategy",{providedIn:"root",factory:()=>{let t=u(R);return()=>Di(t)}}),ZF=new b("MAT_SELECT_CONFIG"),XF=new b("MatSelectTrigger"),Hv=class{source;value;constructor(n,e){this.source=n,this.value=e}},Nf=(()=>{class t{_viewportRuler=u(Ci);_changeDetectorRef=u(Oe);_elementRef=u(O);_dir=u(ct,{optional:!0});_idGenerator=u(Ie);_renderer=u(Re);_parentFormField=u(Vl,{optional:!0});ngControl=u(Hs,{self:!0,optional:!0});_liveAnnouncer=u(wl);_defaultOptions=u(ZF,{optional:!0});_animationsDisabled=Ee();_popoverLocation;_initialized=new x;_cleanupDetach;options;optionGroups;customTrigger;_positions=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"}];_scrollOptionIntoView(e){let i=this.options.toArray()[e];if(i){let r=this.panel.nativeElement,o=yx(e,this.options,this.optionGroups),s=i._getHostElement();e===0&&o===1?r.scrollTop=0:r.scrollTop=Sx(s.offsetTop,s.offsetHeight,r.scrollTop,r.offsetHeight)}}_positioningSettled(){this._scrollOptionIntoView(this._keyManager.activeItemIndex||0)}_getChangeEvent(e){return new Hv(this,e)}_scrollStrategyFactory=u(YF);_panelOpen=!1;_compareWith=(e,i)=>e===i;_uid=this._idGenerator.getId("mat-select-");_triggerAriaLabelledBy=null;_previousControl;_destroy=new x;_errorStateTracker;stateChanges=new x;disableAutomaticLabeling=!0;userAriaDescribedBy;_selectionModel;_keyManager;_preferredOverlayOrigin;_overlayWidth;_onChange=()=>{};_onTouched=()=>{};_valueId=this._idGenerator.getId("mat-select-value-");_scrollStrategy;_overlayPanelClass=this._defaultOptions?.overlayPanelClass||"";get focused(){return this._focused||this._panelOpen}_focused=!1;controlType="mat-select";trigger;panel;_overlayDir;panelClass;disabled=!1;get disableRipple(){return this._disableRipple()}set disableRipple(e){this._disableRipple.set(e)}_disableRipple=U(!1);tabIndex=0;get hideSingleSelectionIndicator(){return this._hideSingleSelectionIndicator}set hideSingleSelectionIndicator(e){this._hideSingleSelectionIndicator=e,this._syncParentProperties()}_hideSingleSelectionIndicator=this._defaultOptions?.hideSingleSelectionIndicator??!1;get placeholder(){return this._placeholder}set placeholder(e){this._placeholder=e,this.stateChanges.next()}_placeholder;get required(){return this._required??this.ngControl?.control?.hasValidator(Eo.required)??!1}set required(e){this._required=e,this.stateChanges.next()}_required;get multiple(){return this._multiple}set multiple(e){this._selectionModel,this._multiple=e}_multiple=!1;disableOptionCentering=this._defaultOptions?.disableOptionCentering??!1;get compareWith(){return this._compareWith}set compareWith(e){this._compareWith=e,this._selectionModel&&this._initializeSelection()}get value(){return this._value}set value(e){this._assignValue(e)&&this._onChange(e)}_value;ariaLabel="";ariaLabelledby;get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}typeaheadDebounceInterval;sortComparator;get id(){return this._id}set id(e){this._id=e||this._uid,this.stateChanges.next()}_id;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}panelWidth=this._defaultOptions&&typeof this._defaultOptions.panelWidth<"u"?this._defaultOptions.panelWidth:"auto";canSelectNullableOptions=this._defaultOptions?.canSelectNullableOptions??!1;optionSelectionChanges=hn(()=>{let e=this.options;return e?e.changes.pipe(nt(e),Ke(()=>Nt(...e.map(i=>i.onSelectionChange)))):this._initialized.pipe(Ke(()=>this.optionSelectionChanges))});openedChange=new W;_openedStream=this.openedChange.pipe(ge(e=>e),oe(()=>{}));_closedStream=this.openedChange.pipe(ge(e=>!e),oe(()=>{}));selectionChange=new W;valueChange=new W;constructor(){let e=u(Tf),i=u(ql,{optional:!0}),r=u(Yl,{optional:!0}),o=u(new Cn("tabindex"),{optional:!0}),s=u(Tl,{optional:!0}),a=u(If,{optional:!0,self:!0});this.ngControl&&(this.ngControl.valueAccessor=this),this._defaultOptions?.typeaheadDebounceInterval!=null&&(this.typeaheadDebounceInterval=this._defaultOptions.typeaheadDebounceInterval),this._errorStateTracker=new Us(e,a||this.ngControl,r,i,this.stateChanges),this._scrollStrategy=this._scrollStrategyFactory(),this.tabIndex=o==null?0:parseInt(o)||0,this._popoverLocation=s?.usePopover===!1?null:"inline",this.id=this.id}ngOnInit(){this._selectionModel=new Do(this.multiple),this.stateChanges.next(),this._viewportRuler.change().pipe(Ce(this._destroy)).subscribe(()=>{this.panelOpen&&(this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._changeDetectorRef.detectChanges())})}ngAfterContentInit(){this._initialized.next(),this._initialized.complete(),this._initKeyManager(),this._selectionModel.changed.pipe(Ce(this._destroy)).subscribe(e=>{e.added.forEach(i=>i.select()),e.removed.forEach(i=>i.deselect())}),this.options.changes.pipe(nt(null),Ce(this._destroy)).subscribe(()=>{this._resetOptions(),this._initializeSelection()})}ngDoCheck(){let e=this._getTriggerAriaLabelledby(),i=this.ngControl;if(e!==this._triggerAriaLabelledBy){let r=this._elementRef.nativeElement;this._triggerAriaLabelledBy=e,e?r.setAttribute("aria-labelledby",e):r.removeAttribute("aria-labelledby")}i&&(this._previousControl!==i.control&&(this._previousControl!==void 0&&i.disabled!==null&&i.disabled!==this.disabled&&(this.disabled=i.disabled),this._previousControl=i.control),this.updateErrorState())}ngOnChanges(e){(e.disabled||e.userAriaDescribedBy)&&this.stateChanges.next(),e.typeaheadDebounceInterval&&this._keyManager&&this._keyManager.withTypeAhead(this.typeaheadDebounceInterval),e.panelClass&&this.panelClass instanceof Set&&(this.panelClass=Array.from(this.panelClass))}ngOnDestroy(){this._cleanupDetach?.(),this._keyManager?.destroy(),this._destroy.next(),this._destroy.complete(),this.stateChanges.complete()}toggle(){this.panelOpen?this.close():this.open()}open(){this._canOpen()&&(this._parentFormField&&(this._preferredOverlayOrigin=this._parentFormField.getConnectedOverlayOrigin()),this._cleanupDetach?.(),this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._panelOpen=!0,this._overlayDir.positionChange.pipe(Ae(1)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this._positioningSettled()}),this._overlayDir.attachOverlay(),this._keyManager.withHorizontalOrientation(null),this._highlightCorrectOption(),this._changeDetectorRef.markForCheck(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!0)))}close(){this._panelOpen&&(this._panelOpen=!1,this._exitAndDetach(),this._keyManager.withHorizontalOrientation(this._isRtl()?"rtl":"ltr"),this._changeDetectorRef.markForCheck(),this._onTouched(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!1)))}_exitAndDetach(){if(this._animationsDisabled||!this.panel){this._detachOverlay();return}this._cleanupDetach?.(),this._cleanupDetach=()=>{i(),clearTimeout(r),this._cleanupDetach=void 0};let e=this.panel.nativeElement,i=this._renderer.listen(e,"animationend",o=>{o.animationName==="_mat-select-exit"&&(this._cleanupDetach?.(),this._detachOverlay())}),r=setTimeout(()=>{this._cleanupDetach?.(),this._detachOverlay()},200);e.classList.add("mat-select-panel-exit")}_detachOverlay(){this._overlayDir.detachOverlay(),this._changeDetectorRef.markForCheck()}writeValue(e){this._assignValue(e)}registerOnChange(e){this._onChange=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e,this._changeDetectorRef.markForCheck(),this.stateChanges.next()}get panelOpen(){return this._panelOpen}get selected(){return this.multiple?this._selectionModel?.selected||[]:this._selectionModel?.selected[0]}get triggerValue(){if(this.empty)return"";if(this._multiple){let e=this._selectionModel.selected.map(i=>i.viewValue);return this._isRtl()&&e.reverse(),e.join(", ")}return this._selectionModel.selected[0].viewValue}updateErrorState(){this._errorStateTracker.updateErrorState()}_isRtl(){return this._dir?this._dir.value==="rtl":!1}_handleKeydown(e){this.disabled||(this.panelOpen?this._handleOpenKeydown(e):this._handleClosedKeydown(e))}_handleClosedKeydown(e){let i=e.keyCode,r=i===40||i===38||i===37||i===39,o=i===13||i===32,s=this._keyManager;if(!s.isTyping()&&o&&!$e(e)||(this.multiple||e.altKey)&&r)e.preventDefault(),this.open();else if(!this.multiple){let a=this.selected;s.onKeydown(e);let l=this.selected;l&&a!==l&&this._liveAnnouncer.announce(l.viewValue,1e4)}}_handleOpenKeydown(e){let i=this._keyManager,r=e.keyCode,o=r===40||r===38,s=i.isTyping();if(o&&e.altKey)e.preventDefault(),this.close();else if(!s&&(r===13||r===32)&&i.activeItem&&!$e(e))e.preventDefault(),i.activeItem._selectViaInteraction();else if(!s&&this._multiple&&r===65&&e.ctrlKey){e.preventDefault();let a=this.options.some(l=>!l.disabled&&!l.selected);this.options.forEach(l=>{l.disabled||(a?l.select():l.deselect())})}else{let a=i.activeItemIndex;i.onKeydown(e),this._multiple&&o&&e.shiftKey&&i.activeItem&&i.activeItemIndex!==a&&i.activeItem._selectViaInteraction()}}_handleOverlayKeydown(e){e.keyCode===27&&!$e(e)&&(e.preventDefault(),this.close())}_onFocus(){this.disabled||(this._focused=!0,this.stateChanges.next())}_onBlur(){this._focused=!1,this._keyManager?.cancelTypeahead(),!this.disabled&&!this.panelOpen&&(this._onTouched(),this._changeDetectorRef.markForCheck(),this.stateChanges.next())}get empty(){return!this._selectionModel||this._selectionModel.isEmpty()}_initializeSelection(){Promise.resolve().then(()=>{this.ngControl&&(this._value=this.ngControl.value),this._setSelectionByValue(this._value),this.stateChanges.next()})}_setSelectionByValue(e){if(this.options.forEach(i=>i.setInactiveStyles()),this._selectionModel.clear(),this.multiple&&e)Array.isArray(e),e.forEach(i=>this._selectOptionByValue(i)),this._sortValues();else{let i=this._selectOptionByValue(e);i?this._keyManager.updateActiveItem(i):this.panelOpen||this._keyManager.updateActiveItem(-1)}this._changeDetectorRef.markForCheck()}_selectOptionByValue(e){let i=this.options.find(r=>{if(this._selectionModel.isSelected(r))return!1;try{return(r.value!=null||this.canSelectNullableOptions)&&this._compareWith(r.value,e)}catch{return!1}});return i&&this._selectionModel.select(i),i}_assignValue(e){return e!==this._value||this._multiple&&Array.isArray(e)?(this.options&&this._setSelectionByValue(e),this._value=e,!0):!1}_skipPredicate=e=>this.panelOpen?!1:e.disabled;_getOverlayWidth(e){return this.panelWidth==="auto"?(e instanceof Ms?e.elementRef:e||this._elementRef).nativeElement.getBoundingClientRect().width:this.panelWidth===null?"":this.panelWidth}_syncParentProperties(){if(this.options)for(let e of this.options)e._changeDetectorRef.markForCheck()}_initKeyManager(){this._keyManager=new Al(this.options).withTypeAhead(this.typeaheadDebounceInterval).withVerticalOrientation().withHorizontalOrientation(this._isRtl()?"rtl":"ltr").withHomeAndEnd().withPageUpDown().withAllowedModifierKeys(["shiftKey"]).skipPredicate(this._skipPredicate),this._keyManager.tabOut.subscribe(()=>{this.panelOpen&&(!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction(),this.focus(),this.close())}),this._keyManager.change.subscribe(()=>{this._panelOpen&&this.panel?this._scrollOptionIntoView(this._keyManager.activeItemIndex||0):!this._panelOpen&&!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction()})}_resetOptions(){let e=Nt(this.options.changes,this._destroy);this.optionSelectionChanges.pipe(Ce(e)).subscribe(i=>{this._onSelect(i.source,i.isUserInput),i.isUserInput&&!this.multiple&&this._panelOpen&&(this.close(),this.focus())}),Nt(...this.options.map(i=>i._stateChanges)).pipe(Ce(e)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this.stateChanges.next()})}_onSelect(e,i){let r=this._selectionModel.isSelected(e);!this.canSelectNullableOptions&&e.value==null&&!this._multiple?(e.deselect(),this._selectionModel.clear(),this.value!=null&&this._propagateChanges(e.value)):(r!==e.selected&&(e.selected?this._selectionModel.select(e):this._selectionModel.deselect(e)),i&&this._keyManager.setActiveItem(e),this.multiple&&(this._sortValues(),i&&this.focus())),r!==this._selectionModel.isSelected(e)&&this._propagateChanges(),this.stateChanges.next()}_sortValues(){if(this.multiple){let e=this.options.toArray();this._selectionModel.sort((i,r)=>this.sortComparator?this.sortComparator(i,r,e):e.indexOf(i)-e.indexOf(r)),this.stateChanges.next()}}_propagateChanges(e){let i;this.multiple?i=this.selected.map(r=>r.value):i=this.selected?this.selected.value:e,this._value=i,this.valueChange.emit(i),this._onChange(i),this.selectionChange.emit(this._getChangeEvent(i)),this._changeDetectorRef.markForCheck()}_highlightCorrectOption(){if(this._keyManager)if(this.empty){let e=-1;for(let i=0;i<this.options.length;i++)if(!this.options.get(i).disabled){e=i;break}this._keyManager.setActiveItem(e)}else this._keyManager.setActiveItem(this._selectionModel.selected[0])}_canOpen(){return!this._panelOpen&&!this.disabled&&this.options?.length>0&&!!this._overlayDir}focus(e){this._elementRef.nativeElement.focus(e)}_getPanelAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||null,i=e?e+" ":"";return this.ariaLabelledby?i+this.ariaLabelledby:e}_getAriaActiveDescendant(){return this.panelOpen&&this._keyManager&&this._keyManager.activeItem?this._keyManager.activeItem.id:null}_getTriggerAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||"";return this.ariaLabelledby&&(e+=" "+this.ariaLabelledby),e||(e=this._valueId),e}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(e){let i=yt(e);i&&(i.tagName==="MAT-OPTION"||i.classList.contains("cdk-overlay-backdrop")||i.closest(".mat-mdc-select-panel"))||(this.focus(),this.open())}get shouldLabelFloat(){return this.panelOpen||!this.empty||this.focused&&!!this.placeholder}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-select"]],contentQueries:function(i,r,o){if(i&1&&Gn(o,XF,5)(o,mr,5)(o,Bv,5),i&2){let s;ce(s=de())&&(r.customTrigger=s.first),ce(s=de())&&(r.options=s),ce(s=de())&&(r.optionGroups=s)}},viewQuery:function(i,r){if(i&1&&Ue(BF,5)(jF,5)(rf,5),i&2){let o;ce(o=de())&&(r.trigger=o.first),ce(o=de())&&(r.panel=o.first),ce(o=de())&&(r._overlayDir=o.first)}},hostAttrs:["role","combobox","aria-haspopup","listbox",1,"mat-mdc-select"],hostVars:21,hostBindings:function(i,r){i&1&&D("keydown",function(s){return r._handleKeydown(s)})("focus",function(){return r._onFocus()})("blur",function(){return r._onBlur()}),i&2&&(re("id",r.id)("tabindex",r.disabled?-1:r.tabIndex)("aria-controls",r.panelOpen?r.id+"-panel":null)("aria-expanded",r.panelOpen)("aria-label",r.ariaLabel||null)("aria-required",r.required.toString())("aria-disabled",r.disabled.toString())("aria-invalid",r.errorState)("aria-activedescendant",r._getAriaActiveDescendant()),B("mat-mdc-select-disabled",r.disabled)("mat-mdc-select-invalid",r.errorState)("mat-mdc-select-required",r.required)("mat-mdc-select-empty",r.empty)("mat-mdc-select-multiple",r.multiple)("mat-select-open",r.panelOpen))},inputs:{userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],panelClass:"panelClass",disabled:[2,"disabled","disabled",V],disableRipple:[2,"disableRipple","disableRipple",V],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:Xr(e)],hideSingleSelectionIndicator:[2,"hideSingleSelectionIndicator","hideSingleSelectionIndicator",V],placeholder:"placeholder",required:[2,"required","required",V],multiple:[2,"multiple","multiple",V],disableOptionCentering:[2,"disableOptionCentering","disableOptionCentering",V],compareWith:"compareWith",value:"value",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],errorStateMatcher:"errorStateMatcher",typeaheadDebounceInterval:[2,"typeaheadDebounceInterval","typeaheadDebounceInterval",Xr],sortComparator:"sortComparator",id:"id",panelWidth:"panelWidth",canSelectNullableOptions:[2,"canSelectNullableOptions","canSelectNullableOptions",V]},outputs:{openedChange:"openedChange",_openedStream:"opened",_closedStream:"closed",selectionChange:"selectionChange",valueChange:"valueChange"},exportAs:["matSelect"],features:[Be([{provide:Ll,useExisting:t},{provide:Vv,useExisting:t}]),at],ngContentSelectors:UF,decls:11,vars:10,consts:[["fallbackOverlayOrigin","cdkOverlayOrigin","trigger",""],["panel",""],["cdk-overlay-origin","",1,"mat-mdc-select-trigger",3,"click"],[1,"mat-mdc-select-value"],[1,"mat-mdc-select-placeholder","mat-mdc-select-min-line"],[1,"mat-mdc-select-value-text"],[1,"mat-mdc-select-arrow-wrapper"],[1,"mat-mdc-select-arrow"],["viewBox","0 0 24 24","width","24px","height","24px","focusable","false","aria-hidden","true"],["d","M7 10l5 5 5-5z"],["cdk-connected-overlay","","cdkConnectedOverlayHasBackdrop","","cdkConnectedOverlayBackdropClass","cdk-overlay-transparent-backdrop",3,"detach","backdropClick","overlayKeydown","cdkConnectedOverlayDisableClose","cdkConnectedOverlayPanelClass","cdkConnectedOverlayScrollStrategy","cdkConnectedOverlayOrigin","cdkConnectedOverlayPositions","cdkConnectedOverlayWidth","cdkConnectedOverlayFlexibleDimensions","cdkConnectedOverlayUsePopover"],[1,"mat-mdc-select-min-line"],["role","listbox","tabindex","-1",1,"mat-mdc-select-panel","mdc-menu-surface","mdc-menu-surface--open",3,"keydown"]],template:function(i,r){if(i&1&&(Pe(HF),m(0,"div",2,0),D("click",function(){return r.open()}),m(3,"div",3),P(4,zF,2,1,"span",4)(5,WF,3,1,"span",5),g(),m(6,"div",6)(7,"div",7),Vi(),m(8,"svg",8),ie(9,"path",9),g()()()(),Dt(10,qF,3,16,"ng-template",10),D("detach",function(){return r.close()})("backdropClick",function(){return r.close()})("overlayKeydown",function(s){return r._handleOverlayKeydown(s)})),i&2){let o=ht(1);_(3),re("id",r._valueId),_(),F(r.empty?4:5),_(6),L("cdkConnectedOverlayDisableClose",!0)("cdkConnectedOverlayPanelClass",r._overlayPanelClass)("cdkConnectedOverlayScrollStrategy",r._scrollStrategy)("cdkConnectedOverlayOrigin",r._preferredOverlayOrigin||o)("cdkConnectedOverlayPositions",r._positions)("cdkConnectedOverlayWidth",r._overlayWidth)("cdkConnectedOverlayFlexibleDimensions",!0)("cdkConnectedOverlayUsePopover",r._popoverLocation)}},dependencies:[Ms,rf],styles:[`@keyframes _mat-select-enter {
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
`],encapsulation:2})}return t})();var kf=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({imports:[an,jv,ve,Si,kn,jv]})}return t})();function KF(t,n){if(t&1){let e=Le();m(0,"div",1)(1,"button",2),D("click",function(){J(e);let r=C();return ee(r.action())}),y(2),g()()}if(t&2){let e=C();_(2),At(" ",e.data.action," ")}}var QF=["label"];function JF(t,n){}var eL=Math.pow(2,31)-1,Zl=class{_overlayRef;instance;containerInstance;_afterDismissed=new x;_afterOpened=new x;_onAction=new x;_durationTimeoutId;_dismissedByAction=!1;constructor(n,e){this._overlayRef=e,this.containerInstance=n,n._onExit.subscribe(()=>this._finishDismiss())}dismiss(){this._afterDismissed.closed||this.containerInstance.exit(),clearTimeout(this._durationTimeoutId)}dismissWithAction(){this._onAction.closed||(this._dismissedByAction=!0,this._onAction.next(),this._onAction.complete(),this.dismiss()),clearTimeout(this._durationTimeoutId)}closeWithAction(){this.dismissWithAction()}_dismissAfter(n){this._durationTimeoutId=setTimeout(()=>this.dismiss(),Math.min(n,eL))}_open(){this._afterOpened.closed||(this._afterOpened.next(),this._afterOpened.complete())}_finishDismiss(){this._overlayRef.dispose(),this._onAction.closed||this._onAction.complete(),this._afterDismissed.next({dismissedByAction:this._dismissedByAction}),this._afterDismissed.complete(),this._dismissedByAction=!1}afterDismissed(){return this._afterDismissed}afterOpened(){return this.containerInstance._onEnter}onAction(){return this._onAction}},Ex=new b("MatSnackBarData"),zs=class{politeness="polite";announcementMessage="";viewContainerRef;duration=0;panelClass;direction;data=null;horizontalPosition="center";verticalPosition="bottom"},tL=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=z({type:t,selectors:[["","matSnackBarLabel",""]],hostAttrs:[1,"mat-mdc-snack-bar-label","mdc-snackbar__label"]})}return t})(),nL=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=z({type:t,selectors:[["","matSnackBarActions",""]],hostAttrs:[1,"mat-mdc-snack-bar-actions","mdc-snackbar__actions"]})}return t})(),iL=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=z({type:t,selectors:[["","matSnackBarAction",""]],hostAttrs:[1,"mat-mdc-snack-bar-action","mdc-snackbar__action"]})}return t})(),rL=(()=>{class t{snackBarRef=u(Zl);data=u(Ex);action(){this.snackBarRef.dismissWithAction()}get hasAction(){return!!this.data.action}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["simple-snack-bar"]],hostAttrs:[1,"mat-mdc-simple-snack-bar"],exportAs:["matSnackBar"],decls:3,vars:2,consts:[["matSnackBarLabel",""],["matSnackBarActions",""],["matButton","","matSnackBarAction","",3,"click"]],template:function(i,r){i&1&&(m(0,"div",0),y(1),g(),P(2,KF,3,1,"div",1)),i&2&&(_(),At(" ",r.data.message,`
`),_(),F(r.hasAction?2:-1))},dependencies:[cr,tL,nL,iL],styles:[`.mat-mdc-simple-snack-bar {
  display: flex;
}
.mat-mdc-simple-snack-bar .mat-mdc-snack-bar-label {
  max-height: 50vh;
  overflow: auto;
}
`],encapsulation:2})}return t})(),Uv="_mat-snack-bar-enter",zv="_mat-snack-bar-exit",oL=(()=>{class t extends nr{_ngZone=u(k);_elementRef=u(O);_changeDetectorRef=u(Oe);_platform=u(be);_animationsDisabled=Ee();snackBarConfig=u(zs);_document=u(j);_trackedModals=new Set;_enterFallback;_exitFallback;_injector=u(R);_announceDelay=150;_announceTimeoutId;_destroyed=!1;_portalOutlet;_onAnnounce=new x;_onExit=new x;_onEnter=new x;_animationState="void";_live;_label;_role;_liveElementId=u(Ie).getId("mat-snack-bar-container-live-");constructor(){super();let e=this.snackBarConfig;e.politeness==="assertive"&&!e.announcementMessage?this._live="assertive":e.politeness==="off"?this._live="off":this._live="polite",this._platform.FIREFOX&&(this._live==="polite"&&(this._role="status"),this._live==="assertive"&&(this._role="alert"))}attachComponentPortal(e){this._assertNotAttached();let i=this._portalOutlet.attachComponentPortal(e);return this._afterPortalAttached(),i}attachTemplatePortal(e){this._assertNotAttached();let i=this._portalOutlet.attachTemplatePortal(e);return this._afterPortalAttached(),i}attachDomPortal=e=>{this._assertNotAttached();let i=this._portalOutlet.attachDomPortal(e);return this._afterPortalAttached(),i};onAnimationEnd(e){e===zv?this._completeExit():e===Uv&&(clearTimeout(this._enterFallback),this._ngZone.run(()=>{this._onEnter.next(),this._onEnter.complete()}))}enter(){this._destroyed||(this._animationState="visible",this._changeDetectorRef.markForCheck(),this._changeDetectorRef.detectChanges(),this._screenReaderAnnounce(),this._animationsDisabled?Je(()=>{this._ngZone.run(()=>queueMicrotask(()=>this.onAnimationEnd(Uv)))},{injector:this._injector}):(clearTimeout(this._enterFallback),this._enterFallback=setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-snack-bar-fallback-visible"),this.onAnimationEnd(Uv)},200)))}exit(){return this._destroyed?Y(void 0):(this._ngZone.run(()=>{this._animationState="hidden",this._changeDetectorRef.markForCheck(),this._elementRef.nativeElement.setAttribute("mat-exit",""),clearTimeout(this._announceTimeoutId),this._animationsDisabled?Je(()=>{this._ngZone.run(()=>queueMicrotask(()=>this.onAnimationEnd(zv)))},{injector:this._injector}):(clearTimeout(this._exitFallback),this._exitFallback=setTimeout(()=>this.onAnimationEnd(zv),200))}),this._onExit)}ngOnDestroy(){this._destroyed=!0,this._clearFromModals(),this._completeExit()}_completeExit(){clearTimeout(this._exitFallback),queueMicrotask(()=>{this._onExit.next(),this._onExit.complete()})}_afterPortalAttached(){let e=this._elementRef.nativeElement,i=this.snackBarConfig.panelClass;i&&(Array.isArray(i)?i.forEach(s=>e.classList.add(s)):e.classList.add(i)),this._exposeToModals();let r=this._label.nativeElement,o="mdc-snackbar__label";r.classList.toggle(o,!r.querySelector(`.${o}`))}_exposeToModals(){let e=this._liveElementId,i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],s=o.getAttribute("aria-owns");this._trackedModals.add(o),s?s.indexOf(e)===-1&&o.setAttribute("aria-owns",s+" "+e):o.setAttribute("aria-owns",e)}}_clearFromModals(){this._trackedModals.forEach(e=>{let i=e.getAttribute("aria-owns");if(i){let r=i.replace(this._liveElementId,"").trim();r.length>0?e.setAttribute("aria-owns",r):e.removeAttribute("aria-owns")}}),this._trackedModals.clear()}_assertNotAttached(){this._portalOutlet.hasAttached()}_screenReaderAnnounce(){this._announceTimeoutId||this._ngZone.runOutsideAngular(()=>{this._announceTimeoutId=setTimeout(()=>{if(this._destroyed)return;let e=this._elementRef.nativeElement,i=e.querySelector("[aria-hidden]"),r=e.querySelector("[aria-live]");if(i&&r){let o=null;this._platform.isBrowser&&document.activeElement instanceof HTMLElement&&i.contains(document.activeElement)&&(o=document.activeElement),i.removeAttribute("aria-hidden"),r.appendChild(i),o?.focus(),this._onAnnounce.next(),this._onAnnounce.complete()}},this._announceDelay)})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-snack-bar-container"]],viewQuery:function(i,r){if(i&1&&Ue(qn,7)(QF,7),i&2){let o;ce(o=de())&&(r._portalOutlet=o.first),ce(o=de())&&(r._label=o.first)}},hostAttrs:[1,"mdc-snackbar","mat-mdc-snack-bar-container"],hostVars:6,hostBindings:function(i,r){i&1&&D("animationend",function(s){return r.onAnimationEnd(s.animationName)})("animationcancel",function(s){return r.onAnimationEnd(s.animationName)}),i&2&&B("mat-snack-bar-container-enter",r._animationState==="visible")("mat-snack-bar-container-exit",r._animationState==="hidden")("mat-snack-bar-container-animations-enabled",!r._animationsDisabled)},features:[Te],decls:6,vars:3,consts:[["label",""],[1,"mdc-snackbar__surface","mat-mdc-snackbar-surface"],[1,"mat-mdc-snack-bar-label"],["aria-hidden","true"],["cdkPortalOutlet",""]],template:function(i,r){i&1&&(m(0,"div",1)(1,"div",2,0)(3,"div",3),Dt(4,JF,0,0,"ng-template",4),g(),ie(5,"div"),g()()),i&2&&(_(5),re("aria-live",r._live)("role",r._role)("id",r._liveElementId))},dependencies:[qn],styles:[`@keyframes _mat-snack-bar-enter {
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
`],encapsulation:2,changeDetection:1})}return t})(),sL=new b("mat-snack-bar-default-options",{providedIn:"root",factory:()=>new zs}),xx=(()=>{class t{_live=u(wl);_injector=u(R);_breakpointObserver=u(ho);_parentSnackBar=u(t,{optional:!0,skipSelf:!0});_defaultConfig=u(sL);_animationsDisabled=Ee();_snackBarRefAtThisLevel=null;simpleSnackBarComponent=rL;snackBarContainerComponent=oL;handsetCssClass="mat-mdc-snack-bar-handset";get _openedSnackBarRef(){let e=this._parentSnackBar;return e?e._openedSnackBarRef:this._snackBarRefAtThisLevel}set _openedSnackBarRef(e){this._parentSnackBar?this._parentSnackBar._openedSnackBarRef=e:this._snackBarRefAtThisLevel=e}openFromComponent(e,i){return this._attach(e,i)}openFromTemplate(e,i){return this._attach(e,i)}open(e,i="",r){let o=v(v({},this._defaultConfig),r);return o.data={message:e,action:i},o.announcementMessage===e&&(o.announcementMessage=void 0),this.openFromComponent(this.simpleSnackBarComponent,o)}dismiss(){this._openedSnackBarRef&&this._openedSnackBarRef.dismiss()}ngOnDestroy(){this._snackBarRefAtThisLevel&&this._snackBarRefAtThisLevel.dismiss()}_attachSnackBarContainer(e,i){let r=i&&i.viewContainerRef&&i.viewContainerRef.injector,o=R.create({parent:r||this._injector,providers:[{provide:zs,useValue:i}]}),s=new En(this.snackBarContainerComponent,i.viewContainerRef,o),a=e.attach(s);return a.instance.snackBarConfig=i,a.instance}_attach(e,i){let r=v(v(v({},new zs),this._defaultConfig),i),o=this._createOverlay(r),s=this._attachSnackBarContainer(o,r),a=new Zl(s,o);if(e instanceof Ct){let l=new xn(e,null,{$implicit:r.data,snackBarRef:a});a.instance=s.attachTemplatePortal(l)}else{let l=this._createInjector(r,a),c=new En(e,void 0,l),d=s.attachComponentPortal(c);a.instance=d.instance}return this._breakpointObserver.observe(Ei.HandsetPortrait).pipe(Ce(o.detachments())).subscribe(l=>{o.overlayElement.classList.toggle(this.handsetCssClass,l.matches)}),r.announcementMessage&&s._onAnnounce.subscribe(()=>{this._live.announce(r.announcementMessage,r.politeness)}),this._animateSnackBar(a,r),this._openedSnackBarRef=a,this._openedSnackBarRef}_animateSnackBar(e,i){e.afterDismissed().subscribe(()=>{this._openedSnackBarRef==e&&(this._openedSnackBarRef=null),i.announcementMessage&&this._live.clear()}),i.duration&&i.duration>0&&e.afterOpened().subscribe(()=>e._dismissAfter(i.duration)),this._openedSnackBarRef?(this._openedSnackBarRef.afterDismissed().subscribe(()=>{e.containerInstance.enter()}),this._openedSnackBarRef.dismiss()):e.containerInstance.enter()}_createOverlay(e){let i=new In;i.direction=e.direction;let r=Yn(this._injector),o=e.direction==="rtl",s=e.horizontalPosition==="left"||e.horizontalPosition==="start"&&!o||e.horizontalPosition==="end"&&o,a=!s&&e.horizontalPosition!=="center";return s?r.left("0"):a?r.right("0"):r.centerHorizontally(),e.verticalPosition==="top"?r.top("0"):r.bottom("0"),i.positionStrategy=r,i.disableAnimations=this._animationsDisabled,Tn(this._injector,i)}_createInjector(e,i){let r=e&&e.viewContainerRef&&e.viewContainerRef.injector;return R.create({parent:r||this._injector,providers:[{provide:Zl,useValue:i},{provide:Ex,useValue:e.data}]})}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();function wx(t,n){let e=new Blob([JSON.stringify(n,null,2)],{type:"application/json"}),i=URL.createObjectURL(e);try{let r=document.createElement("a");r.href=i,r.download=t,r.click()}finally{URL.revokeObjectURL(i)}}function Ix(t){return new Promise((n,e)=>{let i=new FileReader;i.onload=()=>{try{n(JSON.parse(i.result))}catch(r){e(r)}},i.onerror=()=>e(i.error??new Error("Falha ao ler o arquivo.")),i.readAsText(t)})}function aL(){return`${Date.now().toString(36)}-${Math.random().toString(36).slice(2,10)}`}function Mx(t){return{id:aL(),name:t.name,savedAt:new Date().toISOString(),sizePresetKey:t.sizePresetKey,elements:t.elements}}var Tx="zpl-label-studio.saved-labels.v1",lL="zpl-label-studio-etiquetas.json",Xl=class extends Error{},Af=class t{savedLabelsSignal=U(ao(Tx)??[]);savedLabels=this.savedLabelsSignal.asReadonly();save(n,e){let i=Mx({name:n,sizePresetKey:e.sizePreset.key,elements:e.elements}),r=[...this.savedLabelsSignal(),i];return this.persist(r)}remove(n){let e=this.savedLabelsSignal().filter(i=>i.id!==n);return this.persist(e)}toLayout(n){let e=JC(_l(n.sizePresetKey));return N(v({},e),{elements:n.elements})}exportAll(){wx(lL,{savedLabels:this.savedLabelsSignal()})}async importFromFile(n){let e=await Ix(n);if(!e||!Array.isArray(e.savedLabels))throw new Xl("Arquivo n\xE3o \xE9 uma exporta\xE7\xE3o v\xE1lida de etiquetas.");let i=e.savedLabels,r=new Set(this.savedLabelsSignal().map(s=>s.id)),o=i.filter(s=>!r.has(s.id));return this.persist([...this.savedLabelsSignal(),...o]),o.length}persist(n){let e=Ss(Tx,n);return e&&this.savedLabelsSignal.set(n),e}static \u0275fac=function(e){return new(e||t)};static \u0275prov=G({token:t,factory:t.\u0275fac,providedIn:"root"})};function cL(t){return t.toString(16).padStart(2,"0").toUpperCase()}function dL(t){let n="";for(let e=0;e<t.packedRows.length;e++)n+=cL(t.packedRows[e]);return n}function uL(t){let n=t.packedRows.length;return`^GFA,${n},${n},${t.bytesPerRow},${dL(t)}`}function Nx(t){return["^XA",`^PW${t.widthDots}`,`^LL${t.heightDots}`,"^LH0,0","^FO0,0",uL(t),"^FS","^XZ"].join(`
`)}var Rf=class t{constructor(n,e){this.renderer=n;this.printerGateway=e}renderer;printerGateway;async execute(n,e){let i=await this.renderer.render(n),r=Nx(i);await this.printerGateway.sendRaw(e,r)}static \u0275fac=function(e){return new(e||t)(A(Vu),A(_s))};static \u0275prov=G({token:t,factory:t.\u0275fac,providedIn:"root"})};var pr=class t{constructor(n){this.gateway=n}gateway;statusSignal=U("unknown");deviceSignal=U(null);status=this.statusSignal.asReadonly();device=this.deviceSignal.asReadonly();async refresh(){this.statusSignal.set("checking");try{let n=await this.gateway.detectDefaultPrinter();this.deviceSignal.set(n),this.statusSignal.set(n?"connected":"no-printer")}catch(n){this.deviceSignal.set(null),this.statusSignal.set(n instanceof yl?"agent-unavailable":"error")}}static \u0275fac=function(e){return new(e||t)(A(_s))};static \u0275prov=G({token:t,factory:t.\u0275fac,providedIn:"root"})};var fL=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-text-field-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`textarea.cdk-textarea-autosize {
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
`],encapsulation:2})}return t})(),hL={passive:!0},kx=(()=>{class t{_platform=u(be);_ngZone=u(k);_renderer=u(Qe).createRenderer(null,null);_styleLoader=u(ze);_monitoredElements=new Map;monitor(e){if(!this._platform.isBrowser)return We;this._styleLoader.load(fL);let i=Wt(e),r=this._monitoredElements.get(i);if(r)return r.subject;let o=new x,s="cdk-text-field-autofilled",a=c=>{c.animationName==="cdk-text-field-autofill-start"&&!i.classList.contains(s)?(i.classList.add(s),this._ngZone.run(()=>o.next({target:c.target,isAutofilled:!0}))):c.animationName==="cdk-text-field-autofill-end"&&i.classList.contains(s)&&(i.classList.remove(s),this._ngZone.run(()=>o.next({target:c.target,isAutofilled:!1})))},l=this._ngZone.runOutsideAngular(()=>(i.classList.add("cdk-text-field-autofill-monitored"),this._renderer.listen(i,"animationstart",a,hL)));return this._monitoredElements.set(i,{subject:o,unlisten:l}),o}stopMonitoring(e){let i=Wt(e),r=this._monitoredElements.get(i);r&&(r.unlisten(),r.subject.complete(),i.classList.remove("cdk-text-field-autofill-monitored"),i.classList.remove("cdk-text-field-autofilled"),this._monitoredElements.delete(i))}ngOnDestroy(){this._monitoredElements.forEach((e,i)=>this.stopMonitoring(i))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=I({token:t,factory:t.\u0275fac})}return t})();var Ax=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({})}return t})();var Rx=new b("MAT_INPUT_VALUE_ACCESSOR");var mL=["button","checkbox","file","hidden","image","radio","range","reset","submit"],pL=new b("MAT_INPUT_CONFIG"),Of=(()=>{class t{_elementRef=u(O);_platform=u(be);ngControl=u(Hs,{optional:!0,self:!0});_autofillMonitor=u(kx);_ngZone=u(k);_formField=u(Vl,{optional:!0});_renderer=u(Re);_uid=u(Ie).getId("mat-input-");_previousNativeValue;_inputValueAccessor;_signalBasedValueAccessor;_previousPlaceholder=null;_errorStateTracker;_config=u(pL,{optional:!0});_cleanupIosKeyup;_cleanupWebkitWheel;_isServer=!1;_isNativeSelect=!1;_isTextarea=!1;_isInFormField=!1;focused=!1;stateChanges=new x;controlType="mat-input";autofilled=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=ln(e),this.focused&&(this.focused=!1,this.stateChanges.next())}_disabled=!1;get id(){return this._id}set id(e){this._id=e||this._uid}_id;placeholder;name;get required(){return this._required??this.ngControl?.control?.hasValidator(Eo.required)??!1}set required(e){this._required=ln(e)}_required;get type(){return this._type}set type(e){this._type=e||"text",this._validateType(),!this._isTextarea&&vv().has(this._type)&&(this._elementRef.nativeElement.type=this._type)}_type="text";get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}userAriaDescribedBy;get value(){return this._signalBasedValueAccessor?this._signalBasedValueAccessor.value():this._inputValueAccessor.value}set value(e){e!==this.value&&(this._signalBasedValueAccessor?this._signalBasedValueAccessor.value.set(e):this._inputValueAccessor.value=e,this.stateChanges.next())}get readonly(){return this._readonly}set readonly(e){this._readonly=ln(e)}_readonly=!1;disabledInteractive;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}_neverEmptyInputTypes=["date","datetime","datetime-local","month","time","week"].filter(e=>vv().has(e));constructor(){let e=u(ql,{optional:!0}),i=u(Yl,{optional:!0}),r=u(Tf),o=u(Rx,{optional:!0,self:!0}),s=u(If,{optional:!0,self:!0}),a=this._elementRef.nativeElement,l=a.nodeName.toLowerCase();o?Kt(o.value)?this._signalBasedValueAccessor=o:this._inputValueAccessor=o:this._inputValueAccessor=a,this._previousNativeValue=this.value,this.id=this.id,this._platform.IOS&&this._ngZone.runOutsideAngular(()=>{this._cleanupIosKeyup=this._renderer.listen(a,"keyup",this._iOSKeyupListener)}),this._errorStateTracker=new Us(r,s||this.ngControl,i,e,this.stateChanges),this._isServer=!this._platform.isBrowser,this._isNativeSelect=l==="select",this._isTextarea=l==="textarea",this._isInFormField=!!this._formField,this.disabledInteractive=this._config?.disabledInteractive||!1,this._isNativeSelect&&(this.controlType=a.multiple?"mat-native-select-multiple":"mat-native-select"),this._signalBasedValueAccessor&&ut(()=>{this._signalBasedValueAccessor.value(),this.stateChanges.next()})}ngAfterViewInit(){this._platform.isBrowser&&this._autofillMonitor.monitor(this._elementRef.nativeElement).subscribe(e=>{this.autofilled=e.isAutofilled,this.stateChanges.next()})}ngOnChanges(){this.stateChanges.next()}ngOnDestroy(){this.stateChanges.complete(),this._platform.isBrowser&&this._autofillMonitor.stopMonitoring(this._elementRef.nativeElement),this._cleanupIosKeyup?.(),this._cleanupWebkitWheel?.()}ngDoCheck(){this.ngControl&&(this.updateErrorState(),this.ngControl.disabled!==null&&this.ngControl.disabled!==this.disabled&&(this.disabled=this.ngControl.disabled,this.stateChanges.next())),this._dirtyCheckNativeValue(),this._dirtyCheckPlaceholder()}focus(e){this._elementRef.nativeElement.focus(e)}updateErrorState(){this._errorStateTracker.updateErrorState()}_focusChanged(e){if(e!==this.focused){if(!this._isNativeSelect&&e&&this.disabled&&this.disabledInteractive){let i=this._elementRef.nativeElement;i.type==="number"?(i.type="text",i.setSelectionRange(0,0),i.type="number"):i.setSelectionRange(0,0)}this.focused=e,this.stateChanges.next()}}_onInput(){}_dirtyCheckNativeValue(){let e=this._elementRef.nativeElement.value;this._previousNativeValue!==e&&(this._previousNativeValue=e,this.stateChanges.next())}_dirtyCheckPlaceholder(){let e=this._getPlaceholder();if(e!==this._previousPlaceholder){let i=this._elementRef.nativeElement;this._previousPlaceholder=e,e?i.setAttribute("placeholder",e):i.removeAttribute("placeholder")}}_getPlaceholder(){return this.placeholder||null}_validateType(){mL.indexOf(this._type)>-1}_isNeverEmpty(){return this._neverEmptyInputTypes.indexOf(this._type)>-1}_isBadInput(){let e=this._elementRef.nativeElement.validity;return e&&e.badInput}get empty(){return!this._isNeverEmpty()&&!this._elementRef.nativeElement.value&&!this._isBadInput()&&!this.autofilled}get shouldLabelFloat(){if(this._isNativeSelect){let e=this._elementRef.nativeElement,i=e.options[0];return this.focused||e.multiple||!this.empty||!!(e.selectedIndex>-1&&i&&i.label)}else return this.focused&&!this.disabled||!this.empty}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(){this.focused||this.focus()}_isInlineSelect(){let e=this._elementRef.nativeElement;return this._isNativeSelect&&(e.multiple||e.size>1)}_iOSKeyupListener=e=>{let i=e.target;!i.value&&i.selectionStart===0&&i.selectionEnd===0&&(i.setSelectionRange(1,1),i.setSelectionRange(0,0))};_getReadonlyAttribute(){return this._isNativeSelect?null:this.readonly||this.disabled&&this.disabledInteractive?"true":null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=z({type:t,selectors:[["input","matInput",""],["textarea","matInput",""],["select","matNativeControl",""],["input","matNativeControl",""],["textarea","matNativeControl",""]],hostAttrs:[1,"mat-mdc-input-element"],hostVars:21,hostBindings:function(i,r){i&1&&D("focus",function(){return r._focusChanged(!0)})("blur",function(){return r._focusChanged(!1)})("input",function(){return r._onInput()}),i&2&&(Et("id",r.id)("disabled",r.disabled&&!r.disabledInteractive)("required",r.required),re("name",r.name||null)("readonly",r._getReadonlyAttribute())("aria-disabled",r.disabled&&r.disabledInteractive?"true":null)("aria-invalid",r.empty&&r.required?null:r.errorState)("aria-required",r.required)("id",r.id),B("mat-input-server",r._isServer)("mat-mdc-form-field-textarea-control",r._isInFormField&&r._isTextarea)("mat-mdc-form-field-input-control",r._isInFormField)("mat-mdc-input-disabled-interactive",r.disabledInteractive)("mdc-text-field__input",r._isInFormField)("mat-mdc-native-select-inline",r._isInlineSelect()))},inputs:{disabled:"disabled",id:"id",placeholder:"placeholder",name:"name",required:"required",type:"type",errorStateMatcher:"errorStateMatcher",userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],value:"value",readonly:"readonly",disabledInteractive:[2,"disabledInteractive","disabledInteractive",V]},exportAs:["matInput"],features:[Be([{provide:Ll,useExisting:t}]),at]})}return t})(),Pf=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({imports:[kn,kn,Ax,ve]})}return t})();function gL(t,n){if(t&1&&(m(0,"p",3),y(1," Em HTTPS o agente usa um certificado autoassinado. Se a impressora n\xE3o for encontrada, abra "),m(2,"a",11),y(3,"a p\xE1gina do agente"),g(),y(4," uma vez e aceite o certificado \u2014 sem isso a chamada falha sem mensagem nenhuma. "),g()),t&2){let e=C();_(2),L("href",e.sslSupportUrl,qr)}}function vL(t,n){t&1&&(m(0,"mat-error"),y(1),g()),t&2&&(_(),ye(n))}function bL(t,n){t&1&&(m(0,"mat-error"),y(1),g()),t&2&&(_(),ye(n))}var $s=class t{dialogRef=u(As);settings=u(er);connection=u(pr);browserPrintClient=u(tr);isSecure=Ji()==="https";padrao=ys();sslSupportUrl=this.browserPrintClient.sslSupportUrl;hostDraft=U(this.settings.config().host);portDraft=U(this.settings.config().port.toString());hostError=K(()=>this.hostDraft().trim().length===0?"Informe um IP ou nome de host.":null);portError=K(()=>{let n=this.portDraft().trim();if(n.length===0)return"Informe a porta.";let e=Number(n);return Number.isInteger(e)&&e>0&&e<=65535?null:"Porta inv\xE1lida (use um n\xFAmero entre 1 e 65535)."});canSave=K(()=>!this.hostError()&&!this.portError());onHostInput(n){this.hostDraft.set(n)}onPortInput(n){this.portDraft.set(n)}onSave(){this.canSave()&&(this.settings.update({host:this.hostDraft().trim(),port:Number(this.portDraft().trim())}),this.connection.refresh(),this.dialogRef.close())}onReset(){this.settings.reset(),this.hostDraft.set(this.padrao.host),this.portDraft.set(this.padrao.port.toString()),this.connection.refresh(),this.dialogRef.close()}onCancel(){this.dialogRef.close()}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=T({type:t,selectors:[["app-printer-connection-settings-dialog"]],decls:45,vars:15,consts:[["mat-dialog-title",""],[1,"dialog-content"],[1,"protocolo"],[1,"hint"],["appearance","outline",1,"full-width"],["matInput","","type","text",3,"input","value","placeholder"],["matInput","","type","number","min","1","max","65535",3,"input","value","placeholder"],[1,"outro-protocolo"],["align","end"],["mat-button","","type","button",3,"click"],["mat-flat-button","","color","primary","type","button",3,"click","disabled"],["target","_blank","rel","noopener",3,"href"]],template:function(e,i){if(e&1&&(m(0,"h2",0),y(1,"Endere\xE7o da impressora"),g(),m(2,"mat-dialog-content",1)(3,"p",2)(4,"mat-icon"),y(5),g(),m(6,"span"),y(7," Esta p\xE1gina est\xE1 em "),m(8,"strong"),y(9),g(),y(10,", ent\xE3o \xE9 por a\xED que a requisi\xE7\xE3o vai \u2014 e o Browser Print atende nesse protocolo na porta "),m(11,"strong"),y(12),g(),y(13,". "),g()(),P(14,gL,5,1,"p",3),m(15,"p",3),y(16," Deixe "),m(17,"strong"),y(18),g(),y(19," e a porta "),m(20,"strong"),y(21),g(),y(22," se o Zebra Browser Print roda neste computador. Pra alcan\xE7ar uma impressora em outra m\xE1quina da rede (por exemplo, atr\xE1s de um relay), informe o IP/host e a porta dela. "),g(),m(23,"mat-form-field",4)(24,"mat-label"),y(25,"IP ou host"),g(),m(26,"input",5),D("input",function(o){return i.onHostInput(o.target.value)}),g(),P(27,vL,2,1,"mat-error"),g(),m(28,"mat-form-field",4)(29,"mat-label"),y(30,"Porta"),g(),m(31,"input",6),D("input",function(o){return i.onPortInput(o.target.value)}),g(),P(32,bL,2,1,"mat-error"),g(),m(33,"p",7)(34,"mat-icon"),y(35,"info_outline"),g(),m(36,"span"),y(37," Este endere\xE7o vale pra este endere\xE7o do app. Abrir o app por outro endere\xE7o (o IP da rede local em vez do site, por exemplo) tem a pr\xF3pria configura\xE7\xE3o, guardada \xE0 parte pelo navegador. "),g()()(),m(38,"mat-dialog-actions",8)(39,"button",9),D("click",function(){return i.onReset()}),y(40,"Restaurar padr\xE3o"),g(),m(41,"button",9),D("click",function(){return i.onCancel()}),y(42,"Cancelar"),g(),m(43,"button",10),D("click",function(){return i.onSave()}),y(44," Salvar e verificar "),g()()),e&2){let r,o;_(3),B("is-https",i.isSecure),_(2),ye(i.isSecure?"lock":"lock_open"),_(4),ye(i.isSecure?"HTTPS":"HTTP"),_(3),ye(i.padrao.port),_(2),F(i.isSecure?14:-1),_(4),ye(i.padrao.host),_(3),ye(i.padrao.port),_(5),L("value",i.hostDraft())("placeholder",i.padrao.host),_(),F((r=i.hostError())?27:-1,r),_(4),L("value",i.portDraft())("placeholder",i.padrao.port.toString()),_(),F((o=i.portError())?32:-1,o),_(11),L("disabled",!i.canSave())}},dependencies:[Nn,cr,EE,SE,DE,CE,kn,dr,xi,Iv,un,dn,Pf,Of],styles:[".dialog-content[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:.5rem;min-width:min(320px,100%)}.protocolo[_ngcontent-%COMP%]{display:flex;align-items:flex-start;gap:.5rem;margin:0 0 .25rem;padding:.5rem .75rem;border-radius:8px;font-size:.8125rem;line-height:1.45;background:color-mix(in srgb,currentColor 8%,transparent);color:var(--%NS%mat-sys-on-surface-variant, #616161)}.protocolo.is-https[_ngcontent-%COMP%]{color:var(--%NS%mat-sys-primary, #2e7d32)}.protocolo[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{flex-shrink:0;font-size:1.125rem;width:1.125rem;height:1.125rem}.hint[_ngcontent-%COMP%]{font-size:.8125rem;color:var(--%NS%mat-sys-on-surface-variant, #616161);margin:0 0 .5rem}.full-width[_ngcontent-%COMP%]{width:100%}.outro-protocolo[_ngcontent-%COMP%]{display:flex;align-items:flex-start;gap:.5rem;margin:0;padding-top:.25rem;border-top:1px solid var(--%NS%mat-sys-outline-variant, #e0e0e0);font-size:.75rem;line-height:1.45;color:var(--%NS%mat-sys-on-surface-variant, #616161)}.outro-protocolo[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{flex-shrink:0;font-size:1rem;width:1rem;height:1rem}.outro-protocolo[_ngcontent-%COMP%]   code[_ngcontent-%COMP%]{font-size:.75rem}"]})};var _L=["tooltip"],yL=20;var SL=new b("mat-tooltip-scroll-strategy",{providedIn:"root",factory:()=>{let t=u(R);return()=>Di(t,{scrollThrottle:yL})}}),CL=new b("mat-tooltip-default-options",{providedIn:"root",factory:()=>({showDelay:0,hideDelay:0,touchendHideDelay:1500})});var Px="tooltip-panel",DL={passive:!0},EL=8,xL=8,wL=24,IL=200,Kl=(()=>{class t{_elementRef=u(O);_ngZone=u(k);_platform=u(be);_ariaDescriber=u(iE);_focusMonitor=u(sn);_dir=u(ct);_injector=u(R);_viewContainerRef=u(bt);_mediaMatcher=u(xs);_document=u(j);_renderer=u(Re);_animationsDisabled=Ee();_defaultOptions=u(CL,{optional:!0});_overlayRef=null;_tooltipInstance=null;_overlayPanelClass;_portal;_position="below";_positionAtOrigin=!1;_disabled=!1;_tooltipClass;_viewInitialized=!1;_pointerExitEventsInitialized=!1;_tooltipComponent=Fx;_viewportMargin=8;_currentPosition;_cssClassPrefix="mat-mdc";_ariaDescriptionPending=!1;_dirSubscribed=!1;get position(){return this._position}set position(e){e!==this._position&&(this._position=e,this._overlayRef&&(this._updatePosition(this._overlayRef),this._tooltipInstance?.show(0),this._overlayRef.updatePosition()))}get positionAtOrigin(){return this._positionAtOrigin}set positionAtOrigin(e){this._positionAtOrigin=ln(e),this._detach(),this._overlayRef=null}get disabled(){return this._disabled}set disabled(e){let i=ln(e);this._disabled!==i&&(this._disabled=i,i?this.hide(0):this._setupPointerEnterEventsIfNeeded(),this._syncAriaDescription(this.message))}get showDelay(){return this._showDelay}set showDelay(e){this._showDelay=yi(e)}_showDelay;get hideDelay(){return this._hideDelay}set hideDelay(e){this._hideDelay=yi(e),this._tooltipInstance&&(this._tooltipInstance._mouseLeaveHideDelay=this._hideDelay)}_hideDelay;touchGestures="auto";get message(){return this._message}set message(e){let i=this._message;this._message=e!=null?String(e).trim():"",!this._message&&this._isTooltipVisible()?this.hide(0):(this._setupPointerEnterEventsIfNeeded(),this._updateTooltipMessage()),this._syncAriaDescription(i)}_message="";get tooltipClass(){return this._tooltipClass}set tooltipClass(e){this._tooltipClass=e,this._tooltipInstance&&this._setTooltipClass(this._tooltipClass)}_eventCleanups=[];_touchstartTimeout=null;_destroyed=new x;_isDestroyed=!1;constructor(){let e=this._defaultOptions;e&&(this._showDelay=e.showDelay,this._hideDelay=e.hideDelay,e.position&&(this.position=e.position),e.positionAtOrigin&&(this.positionAtOrigin=e.positionAtOrigin),e.touchGestures&&(this.touchGestures=e.touchGestures),e.tooltipClass&&(this.tooltipClass=e.tooltipClass)),this._viewportMargin=EL}ngAfterViewInit(){this._viewInitialized=!0,this._setupPointerEnterEventsIfNeeded(),this._focusMonitor.monitor(this._elementRef).pipe(Ce(this._destroyed)).subscribe(e=>{e?e==="keyboard"&&this._ngZone.run(()=>this.show()):this._ngZone.run(()=>this.hide(0))})}ngOnDestroy(){let e=this._elementRef.nativeElement;this._touchstartTimeout&&clearTimeout(this._touchstartTimeout),this._overlayRef&&(this._overlayRef.dispose(),this._tooltipInstance=null),this._eventCleanups.forEach(i=>i()),this._eventCleanups.length=0,this._destroyed.next(),this._destroyed.complete(),this._isDestroyed=!0,this._ariaDescriber.removeDescription(e,this.message,"tooltip"),this._focusMonitor.stopMonitoring(e)}show(e=this.showDelay,i){if(this.disabled||!this.message||this._isTooltipVisible()){this._tooltipInstance?._cancelPendingAnimations();return}let r=this._createOverlay(i);this._detach(),this._portal=this._portal||new En(this._tooltipComponent,this._viewContainerRef);let o=this._tooltipInstance=r.attach(this._portal).instance;o._triggerElement=this._elementRef.nativeElement,o._mouseLeaveHideDelay=this._hideDelay,o.afterHidden().pipe(Ce(this._destroyed)).subscribe(()=>this._detach()),this._setTooltipClass(this._tooltipClass),this._updateTooltipMessage(),o.show(e)}hide(e=this.hideDelay){let i=this._tooltipInstance;i&&(i.isVisible()?i.hide(e):(i._cancelPendingAnimations(),this._detach()))}toggle(e){this._isTooltipVisible()?this.hide():this.show(void 0,e)}_isTooltipVisible(){return!!this._tooltipInstance&&this._tooltipInstance.isVisible()}_createOverlay(e){if(this._overlayRef){let s=this._overlayRef.getConfig().positionStrategy;if((!this.positionAtOrigin||!e)&&s._origin instanceof O)return this._overlayRef;this._detach()}let i=this._injector.get(rr).getAncestorScrollContainers(this._elementRef),r=`${this._cssClassPrefix}-${Px}`,o=go(this._injector,this.positionAtOrigin?e||this._elementRef:this._elementRef).withTransformOriginOn(`.${this._cssClassPrefix}-tooltip`).withFlexibleDimensions(!1).withViewportMargin(this._viewportMargin).withScrollableContainers(i).withPopoverLocation("global");return o.positionChanges.pipe(Ce(this._destroyed)).subscribe(s=>{this._updateCurrentPositionClass(s.connectionPair),this._tooltipInstance&&s.scrollableViewProperties.isOverlayClipped&&this._tooltipInstance.isVisible()&&this._ngZone.run(()=>this.hide(0))}),this._overlayRef=Tn(this._injector,{direction:this._dir,positionStrategy:o,panelClass:this._overlayPanelClass?[...this._overlayPanelClass,r]:r,scrollStrategy:this._injector.get(SL)(),disableAnimations:this._animationsDisabled,eventPredicate:this._overlayEventPredicate}),this._updatePosition(this._overlayRef),this._overlayRef.detachments().pipe(Ce(this._destroyed)).subscribe(()=>this._detach()),this._overlayRef.outsidePointerEvents().pipe(Ce(this._destroyed)).subscribe(()=>this._tooltipInstance?._handleBodyInteraction()),this._overlayRef.keydownEvents().pipe(Ce(this._destroyed)).subscribe(s=>{s.preventDefault(),s.stopPropagation(),this._ngZone.run(()=>this.hide(0))}),this._defaultOptions?.disableTooltipInteractivity&&this._overlayRef.addPanelClass(`${this._cssClassPrefix}-tooltip-panel-non-interactive`),this._dirSubscribed||(this._dirSubscribed=!0,this._dir.change.pipe(Ce(this._destroyed)).subscribe(()=>{this._overlayRef&&this._updatePosition(this._overlayRef)})),this._overlayRef}_detach(){this._overlayRef&&this._overlayRef.hasAttached()&&this._overlayRef.detach(),this._tooltipInstance=null}_updatePosition(e){let i=e.getConfig().positionStrategy,r=this._getOrigin(),o=this._getOverlayPosition();i.withPositions([this._addOffset(v(v({},r.main),o.main)),this._addOffset(v(v({},r.fallback),o.fallback))])}_addOffset(e){let i=xL,r=!this._dir||this._dir.value=="ltr";return e.originY==="top"?e.offsetY=-i:e.originY==="bottom"?e.offsetY=i:e.originX==="start"?e.offsetX=r?-i:i:e.originX==="end"&&(e.offsetX=r?i:-i),e}_getOrigin(){let e=!this._dir||this._dir.value=="ltr",i=this.position,r;i=="above"||i=="below"?r={originX:"center",originY:i=="above"?"top":"bottom"}:i=="before"||i=="left"&&e||i=="right"&&!e?r={originX:"start",originY:"center"}:(i=="after"||i=="right"&&e||i=="left"&&!e)&&(r={originX:"end",originY:"center"});let{x:o,y:s}=this._invertPosition(r.originX,r.originY);return{main:r,fallback:{originX:o,originY:s}}}_getOverlayPosition(){let e=!this._dir||this._dir.value=="ltr",i=this.position,r;i=="above"?r={overlayX:"center",overlayY:"bottom"}:i=="below"?r={overlayX:"center",overlayY:"top"}:i=="before"||i=="left"&&e||i=="right"&&!e?r={overlayX:"end",overlayY:"center"}:(i=="after"||i=="right"&&e||i=="left"&&!e)&&(r={overlayX:"start",overlayY:"center"});let{x:o,y:s}=this._invertPosition(r.overlayX,r.overlayY);return{main:r,fallback:{overlayX:o,overlayY:s}}}_updateTooltipMessage(){this._tooltipInstance&&(this._tooltipInstance.message=this.message,this._tooltipInstance._markForCheck(),Je(()=>{this._tooltipInstance&&this._overlayRef.updatePosition()},{injector:this._injector}))}_setTooltipClass(e){this._tooltipInstance&&(this._tooltipInstance.tooltipClass=e instanceof Set?Array.from(e):e,this._tooltipInstance._markForCheck())}_invertPosition(e,i){return this.position==="above"||this.position==="below"?i==="top"?i="bottom":i==="bottom"&&(i="top"):e==="end"?e="start":e==="start"&&(e="end"),{x:e,y:i}}_updateCurrentPositionClass(e){let{overlayY:i,originX:r,originY:o}=e,s;if(i==="center"?this._dir&&this._dir.value==="rtl"?s=r==="end"?"left":"right":s=r==="start"?"left":"right":s=i==="bottom"&&o==="top"?"above":"below",s!==this._currentPosition){let a=this._overlayRef;if(a){let l=`${this._cssClassPrefix}-${Px}-`;a.removePanelClass(l+this._currentPosition),a.addPanelClass(l+s)}this._currentPosition=s}}_setupPointerEnterEventsIfNeeded(){this._disabled||!this.message||!this._viewInitialized||this._eventCleanups.length||(this._isTouchPlatform()?this.touchGestures!=="off"&&(this._disableNativeGesturesIfNecessary(),this._addListener("touchstart",e=>{let i=e.targetTouches?.[0],r=i?{x:i.clientX,y:i.clientY}:void 0;this._setupPointerExitEventsIfNeeded(),this._touchstartTimeout&&clearTimeout(this._touchstartTimeout);let o=500;this._touchstartTimeout=setTimeout(()=>{this._touchstartTimeout=null,this.show(void 0,r)},this._defaultOptions?.touchLongPressShowDelay??o)})):this._addListener("mouseenter",e=>{this._setupPointerExitEventsIfNeeded();let i;e.x!==void 0&&e.y!==void 0&&(i=e),this.show(void 0,i)}))}_setupPointerExitEventsIfNeeded(){if(!this._pointerExitEventsInitialized){if(this._pointerExitEventsInitialized=!0,!this._isTouchPlatform())this._addListener("mouseleave",e=>{let i=e.relatedTarget;(!i||!this._overlayRef?.overlayElement.contains(i))&&this.hide()}),this._addListener("wheel",e=>{if(this._isTooltipVisible()){let i=this._document.elementFromPoint(e.clientX,e.clientY),r=this._elementRef.nativeElement;i!==r&&!r.contains(i)&&this.hide()}});else if(this.touchGestures!=="off"){this._disableNativeGesturesIfNecessary();let e=()=>{this._touchstartTimeout&&clearTimeout(this._touchstartTimeout),this.hide(this._defaultOptions?.touchendHideDelay)};this._addListener("touchend",e),this._addListener("touchcancel",e)}}}_addListener(e,i){this._eventCleanups.push(this._renderer.listen(this._elementRef.nativeElement,e,i,DL))}_isTouchPlatform(){let e=this._defaultOptions?.detectHoverCapability;return typeof e=="function"?!e():this._platform.IOS||this._platform.ANDROID?!0:this._platform.isBrowser?!!e&&this._mediaMatcher.matchMedia("(any-hover: none)").matches:!1}_disableNativeGesturesIfNecessary(){let e=this.touchGestures;if(e!=="off"){let i=this._elementRef.nativeElement,r=i.style;(e==="on"||i.nodeName!=="INPUT"&&i.nodeName!=="TEXTAREA")&&(r.userSelect=r.msUserSelect=r.webkitUserSelect=r.MozUserSelect="none"),(e==="on"||!i.draggable)&&(r.webkitUserDrag="none"),r.touchAction="none",r.webkitTapHighlightColor="transparent"}}_syncAriaDescription(e){this._ariaDescriptionPending||(this._ariaDescriptionPending=!0,this._ariaDescriber.removeDescription(this._elementRef.nativeElement,e,"tooltip"),this._isDestroyed||Je({write:()=>{this._ariaDescriptionPending=!1,this.message&&!this.disabled&&this._ariaDescriber.describe(this._elementRef.nativeElement,this.message,"tooltip")}},{injector:this._injector}))}_overlayEventPredicate=e=>e.type==="keydown"?this._isTooltipVisible()&&e.keyCode===27&&!$e(e):!0;static \u0275fac=function(i){return new(i||t)};static \u0275dir=z({type:t,selectors:[["","matTooltip",""]],hostAttrs:[1,"mat-mdc-tooltip-trigger"],hostVars:2,hostBindings:function(i,r){i&2&&B("mat-mdc-tooltip-disabled",r.disabled)},inputs:{position:[0,"matTooltipPosition","position"],positionAtOrigin:[0,"matTooltipPositionAtOrigin","positionAtOrigin"],disabled:[0,"matTooltipDisabled","disabled"],showDelay:[0,"matTooltipShowDelay","showDelay"],hideDelay:[0,"matTooltipHideDelay","hideDelay"],touchGestures:[0,"matTooltipTouchGestures","touchGestures"],message:[0,"matTooltip","message"],tooltipClass:[0,"matTooltipClass","tooltipClass"]},exportAs:["matTooltip"]})}return t})(),Fx=(()=>{class t{_changeDetectorRef=u(Oe);_elementRef=u(O);_isMultiline=!1;message;tooltipClass;_showTimeoutId;_hideTimeoutId;_triggerElement;_mouseLeaveHideDelay;_animationsDisabled=Ee();_tooltip;_closeOnInteraction=!1;_isVisible=!1;_onHide=new x;_showAnimation="mat-mdc-tooltip-show";_hideAnimation="mat-mdc-tooltip-hide";show(e){this._hideTimeoutId!=null&&clearTimeout(this._hideTimeoutId),this._showTimeoutId=setTimeout(()=>{this._toggleVisibility(!0),this._showTimeoutId=void 0},e)}hide(e){this._showTimeoutId!=null&&clearTimeout(this._showTimeoutId),this._hideTimeoutId=setTimeout(()=>{this._toggleVisibility(!1),this._hideTimeoutId=void 0},e)}afterHidden(){return this._onHide}isVisible(){return this._isVisible}ngOnDestroy(){this._cancelPendingAnimations(),this._onHide.complete(),this._triggerElement=null}_handleBodyInteraction(){this._closeOnInteraction&&this.hide(0)}_markForCheck(){this._changeDetectorRef.markForCheck()}_handleMouseLeave({relatedTarget:e}){(!e||!this._triggerElement.contains(e))&&(this.isVisible()?this.hide(this._mouseLeaveHideDelay):this._finalizeAnimation(!1))}_onShow(){this._isMultiline=this._isTooltipMultiline(),this._markForCheck()}_isTooltipMultiline(){let e=this._elementRef.nativeElement.getBoundingClientRect();return e.height>wL&&e.width>=IL}_handleAnimationEnd({animationName:e}){(e===this._showAnimation||e===this._hideAnimation)&&this._finalizeAnimation(e===this._showAnimation)}_cancelPendingAnimations(){this._showTimeoutId!=null&&clearTimeout(this._showTimeoutId),this._hideTimeoutId!=null&&clearTimeout(this._hideTimeoutId),this._showTimeoutId=this._hideTimeoutId=void 0}_finalizeAnimation(e){e?this._closeOnInteraction=!0:this.isVisible()||this._onHide.next()}_toggleVisibility(e){let i=this._tooltip.nativeElement,r=this._showAnimation,o=this._hideAnimation;if(i.classList.remove(e?o:r),i.classList.add(e?r:o),this._isVisible!==e&&(this._isVisible=e,this._changeDetectorRef.markForCheck()),e&&!this._animationsDisabled&&typeof getComputedStyle=="function"){let s=getComputedStyle(i);(s.getPropertyValue("animation-duration")==="0s"||s.getPropertyValue("animation-name")==="none")&&(this._animationsDisabled=!0)}e&&this._onShow(),this._animationsDisabled&&(i.classList.add("_mat-animation-noopable"),this._finalizeAnimation(e))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-tooltip-component"]],viewQuery:function(i,r){if(i&1&&Ue(_L,7),i&2){let o;ce(o=de())&&(r._tooltip=o.first)}},hostAttrs:["aria-hidden","true"],hostBindings:function(i,r){i&1&&D("mouseleave",function(s){return r._handleMouseLeave(s)})},decls:4,vars:5,consts:[["tooltip",""],[1,"mdc-tooltip","mat-mdc-tooltip",3,"animationend"],[1,"mat-mdc-tooltip-surface","mdc-tooltip__surface"]],template:function(i,r){i&1&&(lt(0,"div",1,0),ts("animationend",function(s){return r._handleAnimationEnd(s)}),lt(2,"div",2),y(3),_t()()),i&2&&(xt(r.tooltipClass),B("mdc-tooltip--multiline",r._isMultiline),_(3),ye(r.message))},styles:[`.mat-mdc-tooltip {
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
`],encapsulation:2})}return t})();var Ff=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({imports:[Il,an,ve,Si]})}return t})();function ML(t,n){if(t&1){let e=Le();m(0,"button",2),D("click",function(){J(e);let r=C();return ee(r.onOpenSettings())}),m(1,"mat-icon"),y(2),g(),m(3,"span",3),y(4),g(),m(5,"mat-icon",4),y(6,"settings"),g()()}if(t&2){let e=C();xt(e.presentation().cssClass),re("aria-label","Impressora: "+e.statusText()+". Tocar para configurar o endere\xE7o."),_(2),ye(e.presentation().icon),_(2),ye(e.compactText())}}function TL(t,n){t&1&&(m(0,"span",8),y(1),g()),t&2&&(_(),ye(n))}function NL(t,n){if(t&1){let e=Le();m(0,"button",12),D("click",function(){J(e);let r=C(2);return ee(r.onRetry())}),m(1,"mat-icon"),y(2,"refresh"),g()()}}function kL(t,n){t&1&&(m(0,"a",13),y(1," Instalar Browser Print "),g())}function AL(t,n){if(t&1&&(m(0,"a",14),y(1," Confiar no certificado local "),g()),t&2){let e=C(3);L("href",e.sslSupportUrl,qr)}}function RL(t,n){if(t&1&&(P(0,kL,2,0,"a",13),P(1,AL,2,1,"a",14)),t&2){let e=C(2);F(e.settings.isCustom()?-1:0),_(),F(e.isSecure?1:-1)}}function OL(t,n){if(t&1){let e=Le();m(0,"div",5)(1,"div",6)(2,"mat-icon"),y(3),g(),m(4,"span",7),y(5),g(),P(6,TL,2,1,"span",8),g(),m(7,"div",9),P(8,NL,3,0,"button",10),m(9,"button",11),D("click",function(){J(e);let r=C();return ee(r.onOpenSettings())}),m(10,"mat-icon"),y(11,"settings"),g()(),P(12,RL,2,2),g()()}if(t&2){let e,i=C();xt(i.presentation().cssClass),_(3),ye(i.presentation().icon),_(2),ye(i.statusText()),_(),F((e=i.customAddressLabel())?6:-1,e),_(2),F(i.store.status()!=="connected"&&i.store.status()!=="checking"?8:-1),_(4),F(i.store.status()==="agent-unavailable"?12:-1)}}var Vx={unknown:{icon:"help_outline",label:"Verificando impressora\u2026",cssClass:"is-checking"},checking:{icon:"sync",label:"Verificando impressora\u2026",cssClass:"is-checking"},connected:{icon:"check_circle",label:"",cssClass:"is-connected"},"no-printer":{icon:"print_disabled",label:"Nenhuma impressora padr\xE3o configurada no Browser Print.",cssClass:"is-warning"},"agent-unavailable":{icon:"usb_off",label:"Zebra Browser Print n\xE3o encontrado neste computador.",cssClass:"is-error"},error:{icon:"error",label:"Erro ao verificar a impressora.",cssClass:"is-error"}},Lf=class t{compact=zt(!1,{transform:V});store=u(pr);settings=u(er);browserPrintClient=u(tr);dialog=u(Rs);isSecure=Ji()==="https";sslSupportUrl=this.browserPrintClient.sslSupportUrl;presentation=K(()=>Vx[this.store.status()]??Vx.error);statusText=K(()=>{let n=this.store.device();return this.store.status()==="connected"&&n?`Impressora pronta: ${n.name}`:this.store.status()==="agent-unavailable"&&this.settings.isCustom()?`Sem resposta do servidor de impress\xE3o em ${this.addressLabel()}.`:this.presentation().label});compactText=K(()=>{let n=this.store.device();switch(this.store.status()){case"connected":return n?`${n.name} \xB7 ${this.addressLabel()}`:"Impressora pronta";case"checking":case"unknown":return"Verificando impressora\u2026";case"no-printer":return"Sem impressora padr\xE3o";default:return this.settings.isCustom()?`Sem resposta \xB7 ${this.addressLabel()}`:"Apontar a impressora"}});addressLabel=K(()=>uD(this.settings.config()));customAddressLabel=K(()=>this.settings.isCustom()?this.addressLabel():null);ngOnInit(){this.store.refresh()}onRetry(){this.store.refresh()}onOpenSettings(){this.dialog.open($s)}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=T({type:t,selectors:[["app-printer-status-badge"]],inputs:{compact:[1,"compact"]},decls:2,vars:1,consts:[["type","button",1,"status-chip",3,"class"],[1,"status-badge",3,"class"],["type","button",1,"status-chip",3,"click"],[1,"chip-text"],[1,"chip-caret"],[1,"status-badge"],[1,"status-main"],[1,"status-text"],["matTooltip","Endere\xE7o configurado do agente Zebra Browser Print",1,"custom-address"],[1,"status-actions"],["mat-icon-button","","type","button","matTooltip","Tentar de novo"],["mat-icon-button","","type","button","matTooltip","Configurar IP/porta da impressora na rede",3,"click"],["mat-icon-button","","type","button","matTooltip","Tentar de novo",3,"click"],["href","https://developer.zebra.com/products/printers/browser-print","target","_blank","rel","noopener","matTooltip","Baixe e instale o Zebra Browser Print no site oficial da Zebra",1,"ssl-hint"],["target","_blank","rel","noopener","matTooltip","Abra este link uma vez e aceite o certificado do agente \u2014 sem isso a chamada falha sem mensagem",1,"ssl-hint",3,"href"]],template:function(e,i){e&1&&P(0,ML,7,5,"button",0)(1,OL,13,7,"div",1),e&2&&F(i.compact()?0:1)},dependencies:[Nn,Co,un,dn,Ff,Kl],styles:["[_nghost-%COMP%]{min-width:0}.status-chip[_ngcontent-%COMP%]{display:inline-flex;align-items:center;gap:.375rem;max-width:100%;min-height:40px;padding:0 .75rem;border:0;border-radius:20px;font:inherit;font-size:.8125rem;cursor:pointer;background:color-mix(in srgb,currentColor 12%,transparent)}.status-chip[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:1.125rem;width:1.125rem;height:1.125rem;flex-shrink:0}.status-chip[_ngcontent-%COMP%]   .chip-text[_ngcontent-%COMP%]{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.status-chip[_ngcontent-%COMP%]   .chip-caret[_ngcontent-%COMP%]{opacity:.7}.status-chip.is-connected[_ngcontent-%COMP%]{color:var(--%NS%mat-sys-primary, #2e7d32)}.status-chip.is-warning[_ngcontent-%COMP%]{color:#b26a00}.status-chip.is-error[_ngcontent-%COMP%]{color:var(--%NS%mat-sys-error, #c62828)}.status-chip.is-checking[_ngcontent-%COMP%]{color:var(--%NS%mat-sys-on-surface-variant, #616161)}.status-badge[_ngcontent-%COMP%]{display:flex;flex-wrap:wrap;align-items:center;column-gap:.75rem;row-gap:.375rem;padding:.5rem .875rem;border-radius:14px;font-size:.875rem;background:color-mix(in srgb,currentColor 10%,transparent)}.status-badge[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:1.25rem;width:1.25rem;height:1.25rem;flex-shrink:0}.status-badge.is-connected[_ngcontent-%COMP%]{color:var(--%NS%mat-sys-primary, #2e7d32)}.status-badge.is-warning[_ngcontent-%COMP%]{color:#b26a00}.status-badge.is-error[_ngcontent-%COMP%]{color:var(--%NS%mat-sys-error, #c62828)}.status-badge.is-checking[_ngcontent-%COMP%]{color:var(--%NS%mat-sys-on-surface-variant, #616161)}.status-main[_ngcontent-%COMP%]{display:flex;align-items:center;gap:.5rem;min-width:0}.status-text[_ngcontent-%COMP%]{min-width:0}.status-actions[_ngcontent-%COMP%]{display:flex;flex-wrap:wrap;align-items:center;column-gap:.75rem;row-gap:.25rem}.ssl-hint[_ngcontent-%COMP%]{font-size:.8125rem;display:inline-flex;align-items:center;min-height:40px}.custom-address[_ngcontent-%COMP%]{font-size:.75rem;opacity:.75}"]})};function Bx(t){return{start:t.x,center:t.x+t.width/2,end:t.x+t.width}}function jx(t){return{start:t.y,center:t.y+t.height/2,end:t.y+t.height}}function Hx(t,n){let e=null;for(let i of n)for(let r of[t.start,t.center,t.end])for(let o of[i.start,i.center,i.end]){let s=o-r;Math.abs(s)<=8&&(e===null||Math.abs(s)<Math.abs(e.offset))&&(e={offset:s,guideAt:o})}return e}function Ux(t,n,e){let i=[...n,e],r=i.map(Bx),o=i.map(jx),s=Hx(Bx(t),r),a=Hx(jx(t),o);return{x:t.x+(s?.offset??0),y:t.y+(a?.offset??0),verticalGuides:s?[s.guideAt]:[],horizontalGuides:a?[a.guideAt]:[]}}var Vf=16,zx=24,$x="zpl-label-studio.designer-draft.v1";function PL(){let t=ao($x);return{sizePreset:_l(t?.sizePresetKey??$g),elements:t?.elements??[]}}var $v={vertical:[],horizontal:[]},An=class t{layoutSignal=U(PL());selectedIdSignal=U(null);activeGuidesSignal=U($v);clipboardSignal=U(null);editingIdSignal=U(null);layout=this.layoutSignal.asReadonly();selectedId=this.selectedIdSignal.asReadonly();editingId=this.editingIdSignal.asReadonly();activeGuides=this.activeGuidesSignal.asReadonly();hasClipboard=K(()=>this.clipboardSignal()!==null);constructor(){ut(()=>{let n=this.layoutSignal();Ss($x,{sizePresetKey:n.sizePreset.key,elements:n.elements})})}loadLayout(n){this.layoutSignal.set(n),this.selectedIdSignal.set(null),this.activeGuidesSignal.set($v)}setSizePresetKey(n){this.layoutSignal.update(e=>N(v({},e),{sizePreset:_l(n)}))}addTextElement(){let n=this.layoutSignal(),{x:e,y:i}=this.cascadeCenter(n),r=iD({centerXDots:e,centerYDots:i}),o=this.measureTightTextSize(r.text,r.fontSizeDots,r.bold,r.rotationDeg),{x:s,y:a}=this.keepInsideLabel(n,{x:e-o.widthDots/2,y:i-o.heightDots/2},o.widthDots,o.heightDots),l=N(v({},r),{widthDots:o.widthDots,heightDots:o.heightDots,xDots:s,yDots:a});this.layoutSignal.set(Bu(n,l)),this.select(l.id)}addImageElement(n,e,i){let r=this.layoutSignal(),o=vi(r.sizePreset),s=bi(r.sizePreset),{x:a,y:l}=this.cascadeCenter(r),c=oD({centerXDots:a,centerYDots:l,naturalWidth:e,naturalHeight:i,imageDataUrl:n,maxWidthDots:o*.8,maxHeightDots:s*.8});this.layoutSignal.set(Bu(r,c)),this.select(c.id)}cascadeCenter(n){let i=n.elements.length%5*48;return{x:vi(n.sizePreset)/2+i,y:bi(n.sizePreset)/2+i}}removeElement(n){this.layoutSignal.update(e=>eD(e,n)),this.selectedIdSignal()===n&&this.selectedIdSignal.set(null)}updateElement(n,e){this.layoutSignal.update(i=>tD(i,n,e))}measureTightTextSize(n,e,i,r){let o=Hu();o.font=gs(e,i);let s=lD({lines:ms(n),lineHeight:so(e),font:vs(o),measureLine:c=>bs(o,c)}),{width:a,height:l}=ps(s.width,s.height,r);return{widthDots:Math.max(Vf,Math.ceil(a)),heightDots:Math.max(Vf,Math.ceil(l))}}applyTextChange(n,e){let i=this.layoutSignal(),r=i.elements.find(S=>S.id===n);if(r?.kind!=="text")return;let o=v(v({},r),e),s=this.measureTightTextSize(o.text,o.fontSizeDots,o.bold,o.rotationDeg),a=r.rotationDeg===90||r.rotationDeg===270,l=o.rotationDeg===90||o.rotationDeg===270,c=a!==l?{widthDots:r.heightDots,heightDots:r.widthDots}:r,d=Math.max(c.widthDots,s.widthDots),f=Math.max(c.heightDots,s.heightDots),{x:h,y:p}=this.keepInsideLabel(i,{x:r.xDots,y:r.yDots},d,f);this.updateElement(n,N(v({},e),{widthDots:d,heightDots:f,xDots:h,yDots:p}))}setTextFontSize(n,e){this.applyTextChange(n,{fontSizeDots:e})}setTextContent(n,e){this.applyTextChange(n,{text:e})}setTextBold(n,e){this.applyTextChange(n,{bold:e})}setTextRotation(n,e){this.applyTextChange(n,{rotationDeg:e})}keepInsideLabel(n,e,i,r){let o=vi(n.sizePreset)-i,s=bi(n.sizePreset)-r;return{x:Math.round(Math.max(0,Math.min(e.x,o))),y:Math.round(Math.max(0,Math.min(e.y,s)))}}select(n){this.selectedIdSignal.set(n),this.editingIdSignal()!==n&&this.editingIdSignal.set(null)}startEditing(n){this.selectedIdSignal.set(n),this.editingIdSignal.set(n)}stopEditing(){this.editingIdSignal.set(null)}nudgeElement(n,e,i){let r=this.layoutSignal().elements.find(o=>o.id===n);r&&this.updateElement(n,{xDots:Math.round(r.xDots+e),yDots:Math.round(r.yDots+i)})}moveElementWithSnap(n,e,i){let r=this.layoutSignal(),o=r.elements.find(d=>d.id===n);if(!o)return;let s={x:e,y:i,width:o.widthDots,height:o.heightDots},a=r.elements.filter(d=>d.id!==n).map(d=>({x:d.xDots,y:d.yDots,width:d.widthDots,height:d.heightDots})),l={x:0,y:0,width:vi(r.sizePreset),height:bi(r.sizePreset)},c=Ux(s,a,l);this.updateElement(n,{xDots:Math.round(c.x),yDots:Math.round(c.y)}),this.activeGuidesSignal.set({vertical:c.verticalGuides,horizontal:c.horizontalGuides})}resizeElement(n,e,i){let r=this.layoutSignal().elements.find(l=>l.id===n),o=r?.kind==="text"?this.measureTightTextSize(r.text,r.fontSizeDots,r.bold,r.rotationDeg):null,s=o?o.widthDots:Vf,a=o?o.heightDots:Vf;this.updateElement(n,{widthDots:Math.max(s,Math.round(e)),heightDots:Math.max(a,Math.round(i))})}clearGuides(){this.activeGuidesSignal.set($v)}copySelected(){let n=this.layoutSignal().elements.find(e=>e.id===this.selectedIdSignal());n&&this.clipboardSignal.set(n)}pasteClipboard(){let n=this.clipboardSignal();if(!n)return;let e=rD(n,{xDots:n.xDots+zx,yDots:n.yDots+zx});this.layoutSignal.set(Bu(this.layoutSignal(),e)),this.clipboardSignal.set(e),this.select(e.id)}duplicateSelected(){this.copySelected(),this.pasteClipboard()}static \u0275fac=function(e){return new(e||t)};static \u0275prov=G({token:t,factory:t.\u0275fac})};var FL=["textareaEl"],LL=()=>[];function VL(t,n){if(t&1){let e=Le();m(0,"textarea",6,0),D("blur",function(r){J(e);let o=C(2);return ee(o.onTextBlur(r))})("pointerdown",function(r){return r.stopPropagation()}),g()}if(t&2){let e=C(2);Ut("text-align",e.asText(e.element()).align)("font-weight",e.asText(e.element()).bold?"bold":"normal")("font-size",e.asText(e.element()).fontSizeDots*e.scale(),"px"),L("value",e.asText(e.element()).text)}}function BL(t,n){if(t&1&&(m(0,"div",9),y(1),g()),t&2){let e=n.$implicit,i=C(3);Ut("top",e.top*i.scale(),"px")("left",e.startX*i.scale(),"px")("line-height",i.lineHeightPx(),"px"),_(),ye(e.text)}}function jL(t,n){if(t&1&&(m(0,"div",7),Jt(1,BL,2,7,"div",8,Up),g()),t&2){let e=C(2);Ut("width",e.textContentWidthPx(),"px")("height",e.textContentHeightPx(),"px")("transform",e.textContentTransform())("font-weight",e.asText(e.element()).bold?"bold":"normal")("font-size",e.asText(e.element()).fontSizeDots*e.scale(),"px"),_(),en(e.textLayout()?.lines??zp(10,LL))}}function HL(t,n){if(t&1&&P(0,VL,2,7,"textarea",4)(1,jL,3,11,"div",5),t&2){let e=C();F(e.editing()?0:1)}}function UL(t,n){if(t&1&&ie(0,"img",1),t&2){let e=C();L("src",e.asImage(e.element()).imageDataUrl,qr)}}function zL(t,n){t&1&&(m(0,"div",2)(1,"mat-icon"),y(2,"warning"),g()())}var Bf=class t{store=u(An);element=zt.required();scale=zt.required();labelWidthDots=zt.required();labelHeightDots=zt.required();isOutOfBounds=K(()=>{let n=this.element();return n.xDots<0||n.yDots<0||n.xDots+n.widthDots>this.labelWidthDots()||n.yDots+n.heightDots>this.labelHeightDots()});editing=K(()=>this.store.editingId()===this.element().id);isSelected=K(()=>this.store.selectedId()===this.element().id);textareaRef=ui("textareaEl");effectiveSize=K(()=>{let n=this.element();return n.kind!=="text"?{width:n.widthDots,height:n.heightDots}:ps(n.widthDots,n.heightDots,n.rotationDeg)});lines=K(()=>{let n=this.element();return n.kind==="text"?ms(n.text):[]});textLayout=K(()=>{let n=this.element();if(n.kind!=="text")return null;let e=Hu();return e.font=gs(n.fontSizeDots,n.bold),ju({lines:this.lines(),lineHeight:so(n.fontSizeDots),boxWidth:this.effectiveSize().width,boxHeight:this.effectiveSize().height,align:n.align,verticalAlign:n.verticalAlign,font:vs(e),measureLine:i=>bs(e,i)})});lineHeightPx=K(()=>{let n=this.element();return n.kind==="text"?so(n.fontSizeDots)*this.scale():0});textContentWidthPx=K(()=>this.effectiveSize().width*this.scale());textContentHeightPx=K(()=>this.effectiveSize().height*this.scale());textContentTransform=K(()=>{let n=this.element();return`translate(-50%, -50%) rotate(${n.kind==="text"?n.rotationDeg:0}deg)`});constructor(){ut(()=>{let n=this.textareaRef()?.nativeElement;this.editing()&&n&&(n.focus(),n.select())})}asText(n){return n}asImage(n){return n}onPointerDownMove(n){if(n.target.closest(".resize-handle")||(n.stopPropagation(),this.store.select(this.element().id),this.editing()))return;let e=n.clientX,i=n.clientY,r=this.element().xDots,o=this.element().yDots,s=this.scale(),a=this.element().id,l=d=>{let f=(d.clientX-e)/s,h=(d.clientY-i)/s;this.store.moveElementWithSnap(a,r+f,o+h)},c=()=>{this.store.clearGuides(),window.removeEventListener("pointermove",l),window.removeEventListener("pointerup",c)};window.addEventListener("pointermove",l),window.addEventListener("pointerup",c)}onResizeHandlePointerDown(n){n.stopPropagation(),n.preventDefault();let e=n.clientX,i=n.clientY,r=this.element().widthDots,o=this.element().heightDots,s=this.scale(),a=this.element().id,l=d=>{let f=(d.clientX-e)/s,h=(d.clientY-i)/s;this.store.resizeElement(a,r+f,o+h)},c=()=>{window.removeEventListener("pointermove",l),window.removeEventListener("pointerup",c)};window.addEventListener("pointermove",l),window.addEventListener("pointerup",c)}onDoubleClick(){this.element().kind==="text"&&this.store.startEditing(this.element().id)}onTextBlur(n){let e=n.target.value;this.store.setTextContent(this.element().id,e),this.store.stopEditing()}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=T({type:t,selectors:[["app-label-element-box"]],viewQuery:function(e,i){e&1&&Wi(i.textareaRef,FL,5),e&2&&qi()},hostAttrs:[1,"label-element-box"],hostVars:12,hostBindings:function(e,i){e&1&&D("pointerdown",function(o){return i.onPointerDownMove(o)})("dblclick",function(){return i.onDoubleClick()}),e&2&&(Ut("left",i.element().xDots*i.scale(),"px")("top",i.element().yDots*i.scale(),"px")("width",i.element().widthDots*i.scale(),"px")("height",i.element().heightDots*i.scale(),"px"),B("is-selected",i.isSelected())("is-out-of-bounds",i.isOutOfBounds()))},inputs:{element:[1,"element"],scale:[1,"scale"],labelWidthDots:[1,"labelWidthDots"],labelHeightDots:[1,"labelHeightDots"]},decls:4,vars:2,consts:[["textareaEl",""],["alt","",1,"image-content",3,"src"],["matTooltip","Essa caixa passa da borda da etiqueta \u2014 a parte de fora n\xE3o sai impressa. Mova a caixa, diminua a fonte ou deixe-a menor pra caber.","matTooltipPosition","above",1,"bounds-warning"],[1,"resize-handle",3,"pointerdown"],[1,"text-edit-overlay",3,"value","text-align","font-weight","font-size"],[1,"text-content",3,"width","height","transform","font-weight","font-size"],[1,"text-edit-overlay",3,"blur","pointerdown","value"],[1,"text-content"],[1,"text-line",3,"top","left","line-height"],[1,"text-line"]],template:function(e,i){e&1&&(P(0,HL,2,1)(1,UL,1,1,"img",1),P(2,zL,3,0,"div",2),m(3,"div",3),D("pointerdown",function(o){return i.onResizeHandlePointerDown(o)}),g()),e&2&&(F(i.element().kind==="text"?0:1),_(2),F(i.isOutOfBounds()?2:-1))},dependencies:[un,dn,Ff,Kl],styles:['[_nghost-%COMP%]{position:absolute;box-sizing:border-box;border:1px dashed rgba(0,0,0,.2);cursor:move;-webkit-user-select:none;user-select:none;touch-action:none}.is-selected[_nghost-%COMP%]{border-color:#3f51b5}.is-out-of-bounds[_nghost-%COMP%]{border:2px solid #d32f2f}.text-content[_ngcontent-%COMP%]{position:absolute;top:50%;left:50%;overflow:hidden;color:#000;font-family:Arial,sans-serif}.text-line[_ngcontent-%COMP%]{position:absolute;white-space:pre}.text-edit-overlay[_ngcontent-%COMP%]{width:100%;height:100%;box-sizing:border-box;border:none;outline:none;resize:none;font-family:Arial,sans-serif;padding:0}.image-content[_ngcontent-%COMP%]{width:100%;height:100%;object-fit:fill;display:block;pointer-events:none}.bounds-warning[_ngcontent-%COMP%]{position:absolute;top:2px;right:2px;color:#f57c00;background:#fff;border-radius:50%;line-height:0;cursor:help}.bounds-warning[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:1rem;width:1rem;height:1rem}.resize-handle[_ngcontent-%COMP%]{position:absolute;right:-5px;bottom:-5px;width:10px;height:10px;border-radius:50%;background:#3f51b5;cursor:nwse-resize;display:none}.resize-handle[_ngcontent-%COMP%]:before{content:"";position:absolute;inset:-10px -22px -22px -10px}.is-selected[_nghost-%COMP%]   .resize-handle[_ngcontent-%COMP%]{display:block}']})};var $L=["button"],GL=["*"];function WL(t,n){if(t&1&&(m(0,"div",2),ie(1,"mat-pseudo-checkbox",6),g()),t&2){let e=C();_(),L("disabled",e.disabled)}}var Gx=new b("MAT_BUTTON_TOGGLE_DEFAULT_OPTIONS",{providedIn:"root",factory:()=>({hideSingleSelectionIndicator:!1,hideMultipleSelectionIndicator:!1,disabledInteractive:!1})}),Wx=new b("MatButtonToggleGroup"),qL={provide:Gl,useExisting:Pt(()=>Gv),multi:!0},jf=class{source;value;constructor(n,e){this.source=n,this.value=e}},Gv=(()=>{class t{_changeDetector=u(Oe);_dir=u(ct,{optional:!0});_multiple=!1;_disabled=!1;_disabledInteractive=!1;_selectionModel;_rawValue;_controlValueAccessorChangeFn=()=>{};_onTouched=()=>{};_buttonToggles;appearance;get name(){return this._name}set name(e){this._name=e,this._markButtonsForCheck()}_name=u(Ie).getId("mat-button-toggle-group-");vertical=!1;get value(){let e=this._selectionModel?this._selectionModel.selected:[];return this.multiple?e.map(i=>i.value):e[0]?e[0].value:void 0}set value(e){this._setSelectionByValue(e),this.valueChange.emit(this.value)}valueChange=new W;get selected(){let e=this._selectionModel?this._selectionModel.selected:[];return this.multiple?e:e[0]||null}get multiple(){return this._multiple}set multiple(e){this._multiple=e,this._markButtonsForCheck()}get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._markButtonsForCheck()}get disabledInteractive(){return this._disabledInteractive}set disabledInteractive(e){this._disabledInteractive=e,this._markButtonsForCheck()}get dir(){return this._dir&&this._dir.value==="rtl"?"rtl":"ltr"}change=new W;get hideSingleSelectionIndicator(){return this._hideSingleSelectionIndicator}set hideSingleSelectionIndicator(e){this._hideSingleSelectionIndicator=e,this._markButtonsForCheck()}_hideSingleSelectionIndicator;get hideMultipleSelectionIndicator(){return this._hideMultipleSelectionIndicator}set hideMultipleSelectionIndicator(e){this._hideMultipleSelectionIndicator=e,this._markButtonsForCheck()}_hideMultipleSelectionIndicator;constructor(){let e=u(Gx,{optional:!0});this.appearance=e&&e.appearance?e.appearance:"standard",this._hideSingleSelectionIndicator=e?.hideSingleSelectionIndicator??!1,this._hideMultipleSelectionIndicator=e?.hideMultipleSelectionIndicator??!1}ngOnInit(){this._selectionModel=new Do(this.multiple,void 0,!1)}ngAfterContentInit(){this._selectionModel.select(...this._buttonToggles.filter(e=>e.checked)),this.multiple||this._initializeTabIndex()}writeValue(e){this.value=e,this._changeDetector.markForCheck()}registerOnChange(e){this._controlValueAccessorChangeFn=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e}_keydown(e){if(this.multiple||this.disabled||$e(e))return;let r=e.target.id,o=this._buttonToggles.toArray().findIndex(a=>a.buttonId===r),s=null;switch(e.keyCode){case 32:case 13:s=this._buttonToggles.get(o)||null;break;case 38:s=this._getNextButton(o,-1);break;case 37:s=this._getNextButton(o,this.dir==="ltr"?-1:1);break;case 40:s=this._getNextButton(o,1);break;case 39:s=this._getNextButton(o,this.dir==="ltr"?1:-1);break;default:return}s&&(e.preventDefault(),s._onButtonClick(),s.focus())}_emitChangeEvent(e){let i=new jf(e,this.value);this._rawValue=i.value,this._controlValueAccessorChangeFn(i.value),this.change.emit(i)}_syncButtonToggle(e,i,r=!1,o=!1){!this.multiple&&this.selected&&!e.checked&&(this.selected.checked=!1),this._selectionModel?i?this._selectionModel.select(e):this._selectionModel.deselect(e):o=!0,o?Promise.resolve().then(()=>this._updateModelValue(e,r)):this._updateModelValue(e,r)}_isSelected(e){return this._selectionModel&&this._selectionModel.isSelected(e)}_isPrechecked(e){return typeof this._rawValue>"u"?!1:this.multiple&&Array.isArray(this._rawValue)?this._rawValue.some(i=>e.value!=null&&i===e.value):e.value===this._rawValue}_initializeTabIndex(){if(this._buttonToggles.forEach(e=>{e.tabIndex=-1}),this.selected)this.selected.tabIndex=0;else for(let e=0;e<this._buttonToggles.length;e++){let i=this._buttonToggles.get(e);if(!i.disabled){i.tabIndex=0;break}}}_getNextButton(e,i){let r=this._buttonToggles;for(let o=1;o<=r.length;o++){let s=(e+i*o+r.length)%r.length,a=r.get(s);if(a&&!a.disabled)return a}return null}_setSelectionByValue(e){if(this._rawValue=e,!this._buttonToggles)return;let i=this._buttonToggles.toArray();if(this.multiple&&e?(Array.isArray(e),this._clearSelection(),e.forEach(r=>this._selectValue(r,i))):(this._clearSelection(),this._selectValue(e,i)),!this.multiple&&i.every(r=>r.tabIndex===-1)){for(let r of i)if(!r.disabled){r.tabIndex=0;break}}}_clearSelection(){this._selectionModel.clear(),this._buttonToggles.forEach(e=>{e.checked=!1,this.multiple||(e.tabIndex=-1)})}_selectValue(e,i){for(let r of i)if(r.value===e){r.checked=!0,this._selectionModel.select(r),this.multiple||(r.tabIndex=0);break}}_updateModelValue(e,i){i&&this._emitChangeEvent(e),this.valueChange.emit(this.value)}_markButtonsForCheck(){this._buttonToggles?.forEach(e=>e._markForCheck())}static \u0275fac=function(i){return new(i||t)};static \u0275dir=z({type:t,selectors:[["mat-button-toggle-group"]],contentQueries:function(i,r,o){if(i&1&&Gn(o,Hf,5),i&2){let s;ce(s=de())&&(r._buttonToggles=s)}},hostAttrs:[1,"mat-button-toggle-group"],hostVars:6,hostBindings:function(i,r){i&1&&D("keydown",function(s){return r._keydown(s)}),i&2&&(re("role",r.multiple?"group":"radiogroup")("aria-disabled",r.disabled),B("mat-button-toggle-vertical",r.vertical)("mat-button-toggle-group-appearance-standard",r.appearance==="standard"))},inputs:{appearance:"appearance",name:"name",vertical:[2,"vertical","vertical",V],value:"value",multiple:[2,"multiple","multiple",V],disabled:[2,"disabled","disabled",V],disabledInteractive:[2,"disabledInteractive","disabledInteractive",V],hideSingleSelectionIndicator:[2,"hideSingleSelectionIndicator","hideSingleSelectionIndicator",V],hideMultipleSelectionIndicator:[2,"hideMultipleSelectionIndicator","hideMultipleSelectionIndicator",V]},outputs:{valueChange:"valueChange",change:"change"},exportAs:["matButtonToggleGroup"],features:[Be([qL,{provide:Wx,useExisting:t}])]})}return t})(),Hf=(()=>{class t{_changeDetectorRef=u(Oe);_elementRef=u(O);_focusMonitor=u(sn);_idGenerator=u(Ie);_animationDisabled=Ee();_checked=!1;ariaLabel;ariaLabelledby=null;_buttonElement;buttonToggleGroup;get buttonId(){return`${this.id}-button`}id;name;value;get tabIndex(){return this._tabIndex()}set tabIndex(e){this._tabIndex.set(e)}_tabIndex;disableRipple=!1;get appearance(){return this.buttonToggleGroup?this.buttonToggleGroup.appearance:this._appearance}set appearance(e){this._appearance=e}_appearance;get checked(){return this.buttonToggleGroup?this.buttonToggleGroup._isSelected(this):this._checked}set checked(e){e!==this._checked&&(this._checked=e,this.buttonToggleGroup&&this.buttonToggleGroup._syncButtonToggle(this,this._checked),this._changeDetectorRef.markForCheck())}get disabled(){return this._disabled||this.buttonToggleGroup&&this.buttonToggleGroup.disabled}set disabled(e){this._disabled=e}_disabled=!1;get disabledInteractive(){return this._disabledInteractive||this.buttonToggleGroup!==null&&this.buttonToggleGroup.disabledInteractive}set disabledInteractive(e){this._disabledInteractive=e}_disabledInteractive;change=new W;constructor(){u(ze).load(Zn);let e=u(Wx,{optional:!0}),i=u(new Cn("tabindex"),{optional:!0})||"",r=u(Gx,{optional:!0});this._tabIndex=U(parseInt(i)||0),this.buttonToggleGroup=e,this._appearance=r&&r.appearance?r.appearance:"standard",this._disabledInteractive=r?.disabledInteractive??!1}ngOnInit(){let e=this.buttonToggleGroup;this.id=this.id||this._idGenerator.getId("mat-button-toggle-"),e&&(e._isPrechecked(this)?this.checked=!0:e._isSelected(this)!==this._checked&&e._syncButtonToggle(this,this._checked))}ngAfterViewInit(){this._animationDisabled||this._elementRef.nativeElement.classList.add("mat-button-toggle-animations-enabled"),this._focusMonitor.monitor(this._elementRef,!0)}ngOnDestroy(){let e=this.buttonToggleGroup;this._focusMonitor.stopMonitoring(this._elementRef),e&&e._isSelected(this)&&e._syncButtonToggle(this,!1,!1,!0)}focus(e){this._buttonElement.nativeElement.focus(e)}_onButtonClick(){if(this.disabled)return;let e=this.isSingleSelector()?!0:!this._checked;if(e!==this._checked&&(this._checked=e,this.buttonToggleGroup&&(this.buttonToggleGroup._syncButtonToggle(this,this._checked,!0),this.buttonToggleGroup._onTouched())),this.isSingleSelector()){let i=this.buttonToggleGroup._buttonToggles.find(r=>r.tabIndex===0);i&&(i.tabIndex=-1),this.tabIndex=0}this.change.emit(new jf(this,this.value))}_markForCheck(){this._changeDetectorRef.markForCheck()}_getButtonName(){return this.isSingleSelector()?this.buttonToggleGroup.name:this.name||null}isSingleSelector(){return this.buttonToggleGroup&&!this.buttonToggleGroup.multiple}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-button-toggle"]],viewQuery:function(i,r){if(i&1&&Ue($L,5),i&2){let o;ce(o=de())&&(r._buttonElement=o.first)}},hostAttrs:["role","presentation",1,"mat-button-toggle"],hostVars:14,hostBindings:function(i,r){i&1&&D("focus",function(){return r.focus()}),i&2&&(re("aria-label",null)("aria-labelledby",null)("id",r.id)("name",null),B("mat-button-toggle-standalone",!r.buttonToggleGroup)("mat-button-toggle-checked",r.checked)("mat-button-toggle-disabled",r.disabled)("mat-button-toggle-disabled-interactive",r.disabledInteractive)("mat-button-toggle-appearance-standard",r.appearance==="standard"))},inputs:{ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],id:"id",name:"name",value:"value",tabIndex:"tabIndex",disableRipple:[2,"disableRipple","disableRipple",V],appearance:"appearance",checked:[2,"checked","checked",V],disabled:[2,"disabled","disabled",V],disabledInteractive:[2,"disabledInteractive","disabledInteractive",V]},outputs:{change:"change"},exportAs:["matButtonToggle"],ngContentSelectors:GL,decls:7,vars:13,consts:[["button",""],["type","button",1,"mat-button-toggle-button","mat-focus-indicator",3,"click","id","disabled"],[1,"mat-button-toggle-checkbox-wrapper"],[1,"mat-button-toggle-label-content"],[1,"mat-button-toggle-focus-overlay"],["matRipple","",1,"mat-button-toggle-ripple",3,"matRippleTrigger","matRippleDisabled"],["state","checked","aria-hidden","true","appearance","minimal",3,"disabled"]],template:function(i,r){if(i&1&&(Pe(),m(0,"button",1,0),D("click",function(){return r._onButtonClick()}),P(2,WL,2,1,"div",2),m(3,"span",3),me(4),g()(),ie(5,"span",4)(6,"span",5)),i&2){let o=ht(1);L("id",r.buttonId)("disabled",r.disabled&&!r.disabledInteractive||null),re("role",r.isSingleSelector()?"radio":"button")("tabindex",r.disabled&&!r.disabledInteractive?-1:r.tabIndex)("aria-pressed",r.isSingleSelector()?null:r.checked)("aria-checked",r.isSingleSelector()?r.checked:null)("name",r._getButtonName())("aria-label",r.ariaLabel)("aria-labelledby",r.ariaLabelledby)("aria-disabled",r.disabled&&r.disabledInteractive?"true":null),_(2),F(r.buttonToggleGroup&&(!r.buttonToggleGroup.multiple&&!r.buttonToggleGroup.hideSingleSelectionIndicator||r.buttonToggleGroup.multiple&&!r.buttonToggleGroup.hideMultipleSelectionIndicator)?2:-1),_(4),L("matRippleTrigger",o)("matRippleDisabled",r.disableRipple||r.disabled)}},dependencies:[ar,Mf],styles:[`.mat-button-toggle-standalone,
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
`],encapsulation:2})}return t})(),qx=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({imports:[lr,Hf,ve]})}return t})();var ZL=["*"],Yx=(()=>{class t{labelPosition="after";static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["","mat-internal-form-field",""]],hostAttrs:[1,"mdc-form-field","mat-internal-form-field"],hostVars:2,hostBindings:function(i,r){i&2&&B("mdc-form-field--align-end",r.labelPosition==="before")},inputs:{labelPosition:"labelPosition"},ngContentSelectors:ZL,decls:1,vars:0,template:function(i,r){i&1&&(Pe(),me(0))},styles:[`.mat-internal-form-field {
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
`],encapsulation:2})}return t})();var XL=["input"],KL=["*"],Wv={color:"accent",clickAction:"check-indeterminate",disabledInteractive:!1},QL=new b("mat-checkbox-default-options",{providedIn:"root",factory:()=>Wv}),It=(function(t){return t[t.Init=0]="Init",t[t.Checked=1]="Checked",t[t.Unchecked=2]="Unchecked",t[t.Indeterminate=3]="Indeterminate",t})(It||{}),qv=class{source;checked},Yv=(()=>{class t{_elementRef=u(O);_changeDetectorRef=u(Oe);_ngZone=u(k);_animationsDisabled=Ee();_options=u(QL,{optional:!0});focus(){this._inputElement.nativeElement.focus()}_createChangeEvent(e){let i=new qv;return i.source=this,i.checked=e,i}_getAnimationTargetElement(){return this._inputElement?.nativeElement}_animationClasses={uncheckedToChecked:"mdc-checkbox--anim-unchecked-checked",uncheckedToIndeterminate:"mdc-checkbox--anim-unchecked-indeterminate",checkedToUnchecked:"mdc-checkbox--anim-checked-unchecked",checkedToIndeterminate:"mdc-checkbox--anim-checked-indeterminate",indeterminateToChecked:"mdc-checkbox--anim-indeterminate-checked",indeterminateToUnchecked:"mdc-checkbox--anim-indeterminate-unchecked"};ariaLabel="";ariaLabelledby=null;ariaDescribedby;ariaExpanded;ariaControls;ariaOwns;_uniqueId;id;get inputId(){return`${this.id||this._uniqueId}-input`}required=!1;labelPosition="after";name=null;change=new W;indeterminateChange=new W;value;disableRipple=!1;_inputElement;tabIndex;color;disabledInteractive;_onTouched=()=>{};_currentAnimationClass="";_currentCheckState=It.Init;_controlValueAccessorChangeFn=()=>{};_validatorChangeFn=()=>{};constructor(){u(ze).load(Zn);let e=u(new Cn("tabindex"),{optional:!0});this._options=this._options||Wv,this.color=this._options.color||Wv.color,this.tabIndex=e==null?0:parseInt(e)||0,this.id=this._uniqueId=u(Ie).getId("mat-mdc-checkbox-"),this.disabledInteractive=this._options?.disabledInteractive??!1}ngOnChanges(e){e.required&&this._validatorChangeFn()}ngAfterViewInit(){this._syncIndeterminate(this.indeterminate)}get checked(){return this._checked}set checked(e){e!=this.checked&&(this._checked=e,this._changeDetectorRef.markForCheck())}_checked=!1;get disabled(){return this._disabled}set disabled(e){e!==this.disabled&&(this._disabled=e,this._changeDetectorRef.markForCheck())}_disabled=!1;get indeterminate(){return this._indeterminate()}set indeterminate(e){let i=e!=this._indeterminate();this._indeterminate.set(e),i&&(e?this._transitionCheckState(It.Indeterminate):this._transitionCheckState(this.checked?It.Checked:It.Unchecked),this.indeterminateChange.emit(e)),this._syncIndeterminate(e)}_indeterminate=U(!1);_isRippleDisabled(){return this.disableRipple||this.disabled}_onLabelTextChange(){this._changeDetectorRef.detectChanges()}writeValue(e){this.checked=!!e}registerOnChange(e){this._controlValueAccessorChangeFn=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e}validate(e){return this.required&&e.value!==!0?{required:!0}:null}registerOnValidatorChange(e){this._validatorChangeFn=e}_transitionCheckState(e){let i=this._currentCheckState,r=this._getAnimationTargetElement();if(!(i===e||!r)&&(this._currentAnimationClass&&r.classList.remove(this._currentAnimationClass),this._currentAnimationClass=this._getAnimationClassForCheckStateTransition(i,e),this._currentCheckState=e,this._currentAnimationClass.length>0)){r.classList.add(this._currentAnimationClass);let o=this._currentAnimationClass;this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{r.classList.remove(o)},1e3)})}}_emitChangeEvent(){this._controlValueAccessorChangeFn(this.checked),this.change.emit(this._createChangeEvent(this.checked)),this._inputElement&&(this._inputElement.nativeElement.checked=this.checked)}toggle(){this.checked=!this.checked,this._controlValueAccessorChangeFn(this.checked)}_handleInputClick(){let e=this._options?.clickAction;!this.disabled&&e!=="noop"?(this.indeterminate&&e!=="check"&&Promise.resolve().then(()=>{this._indeterminate.set(!1),this.indeterminateChange.emit(!1)}),this._checked=!this._checked,this._transitionCheckState(this._checked?It.Checked:It.Unchecked),this._emitChangeEvent()):(this.disabled&&this.disabledInteractive||!this.disabled&&e==="noop")&&(this._inputElement.nativeElement.checked=this.checked,this._inputElement.nativeElement.indeterminate=this.indeterminate)}_onInteractionEvent(e){e.stopPropagation()}_onBlur(){Promise.resolve().then(()=>{this._onTouched(),this._changeDetectorRef.markForCheck()})}_getAnimationClassForCheckStateTransition(e,i){if(this._animationsDisabled)return"";switch(e){case It.Init:if(i===It.Checked)return this._animationClasses.uncheckedToChecked;if(i==It.Indeterminate)return this._checked?this._animationClasses.checkedToIndeterminate:this._animationClasses.uncheckedToIndeterminate;break;case It.Unchecked:return i===It.Checked?this._animationClasses.uncheckedToChecked:this._animationClasses.uncheckedToIndeterminate;case It.Checked:return i===It.Unchecked?this._animationClasses.checkedToUnchecked:this._animationClasses.checkedToIndeterminate;case It.Indeterminate:return i===It.Checked?this._animationClasses.indeterminateToChecked:this._animationClasses.indeterminateToUnchecked}return""}_syncIndeterminate(e){let i=this._inputElement;i&&(i.nativeElement.indeterminate=e)}_onInputClick(){this._handleInputClick()}_preventBubblingFromLabel(e){e.target&&this._inputElement&&e.target!==this._inputElement.nativeElement&&e.stopPropagation()}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-checkbox"]],viewQuery:function(i,r){if(i&1&&Ue(XL,5),i&2){let o;ce(o=de())&&(r._inputElement=o.first)}},hostAttrs:[1,"mat-mdc-checkbox"],hostVars:16,hostBindings:function(i,r){i&2&&(Et("id",r.id),re("tabindex",null)("aria-label",null)("aria-labelledby",null),xt(r.color?"mat-"+r.color:"mat-accent"),B("_mat-animation-noopable",r._animationsDisabled)("mdc-checkbox--disabled",r.disabled)("mat-mdc-checkbox-disabled",r.disabled)("mat-mdc-checkbox-checked",r.checked)("mat-mdc-checkbox-disabled-interactive",r.disabledInteractive))},inputs:{ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],ariaExpanded:[2,"aria-expanded","ariaExpanded",V],ariaControls:[0,"aria-controls","ariaControls"],ariaOwns:[0,"aria-owns","ariaOwns"],id:"id",required:[2,"required","required",V],labelPosition:"labelPosition",name:"name",value:"value",disableRipple:[2,"disableRipple","disableRipple",V],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?void 0:Xr(e)],color:"color",disabledInteractive:[2,"disabledInteractive","disabledInteractive",V],checked:[2,"checked","checked",V],disabled:[2,"disabled","disabled",V],indeterminate:[2,"indeterminate","indeterminate",V]},outputs:{change:"change",indeterminateChange:"indeterminateChange"},exportAs:["matCheckbox"],features:[Be([{provide:Gl,useExisting:Pt(()=>t),multi:!0},{provide:Wl,useExisting:t,multi:!0}]),at],ngContentSelectors:KL,decls:15,vars:23,consts:[["checkbox",""],["input",""],["label",""],["mat-internal-form-field","",3,"click","labelPosition","for"],[1,"mdc-checkbox"],["aria-hidden","true",1,"mat-mdc-checkbox-touch-target"],["type","checkbox",1,"mdc-checkbox__native-control",3,"blur","click","change","checked","indeterminate","disabled","id","required","tabIndex"],["aria-hidden","true",1,"mdc-checkbox__ripple"],["aria-hidden","true",1,"mdc-checkbox__background"],["focusable","false","viewBox","0 0 24 24",1,"mdc-checkbox__checkmark"],["fill","none","d","M1.73,12.91 8.1,19.28 22.79,4.59",1,"mdc-checkbox__checkmark-path"],[1,"mdc-checkbox__mixedmark"],["mat-ripple","","aria-hidden","true",1,"mat-mdc-checkbox-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled","matRippleCentered"],[1,"mat-internal-form-field-label","mdc-label"]],template:function(i,r){if(i&1&&(Pe(),m(0,"label",3),D("click",function(s){return r._preventBubblingFromLabel(s)}),m(1,"span",4,0),ie(3,"span",5),m(4,"input",6,1),D("blur",function(){return r._onBlur()})("click",function(){return r._onInputClick()})("change",function(s){return r._onInteractionEvent(s)}),g(),ie(6,"span",7),m(7,"span",8),Vi(),m(8,"svg",9),ie(9,"path",10),g(),ud(),ie(10,"span",11),g(),ie(11,"span",12),g(),m(12,"span",13,2),me(14),g()()),i&2){let o=ht(2);L("labelPosition",r.labelPosition)("for",r.inputId),_(4),B("mdc-checkbox--selected",r.checked),L("checked",r.checked)("indeterminate",r.indeterminate)("disabled",r.disabled&&!r.disabledInteractive)("id",r.inputId)("required",r.required)("tabIndex",r.disabled&&!r.disabledInteractive?-1:r.tabIndex),re("aria-label",r.ariaLabel||null)("aria-labelledby",r.ariaLabelledby)("aria-describedby",r.ariaDescribedby)("aria-checked",r.indeterminate?"mixed":null)("aria-controls",r.ariaControls)("aria-disabled",r.disabled&&r.disabledInteractive?!0:null)("aria-expanded",r.ariaExpanded)("aria-owns",r.ariaOwns)("name",r.name)("value",r.value),_(7),L("matRippleTrigger",o)("matRippleDisabled",r.disableRipple||r.disabled)("matRippleCentered",!0)}},dependencies:[ar,Yx],styles:[`.mdc-checkbox {
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
`],encapsulation:2})}return t})(),Zx=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=X({type:t});static \u0275inj=q({imports:[Yv,ve]})}return t})();function eV(t,n){if(t&1){let e=Le();m(0,"button",15),D("click",function(){J(e);let r=C(2);return ee(r.onEditText())}),m(1,"mat-icon"),y(2,"edit"),g(),y(3," Editar texto "),g()}}function tV(t,n){if(t&1){let e=Le();m(0,"mat-form-field",16)(1,"mat-label"),y(2,"Tamanho da fonte (pontos)"),g(),m(3,"input",17),D("input",function(r){J(e);let o=C(2);return ee(o.onFontSizeChange(r.target.valueAsNumber))}),g()(),m(4,"mat-checkbox",18),D("change",function(r){J(e);let o=C(2);return ee(o.onBoldChange(r.checked))}),y(5," Negrito "),g(),m(6,"p",19),y(7,"Alinhamento horizontal do texto"),g(),m(8,"mat-button-toggle-group",20),D("change",function(r){J(e);let o=C(2);return ee(o.onAlignChange(r.value))}),m(9,"mat-button-toggle",21)(10,"mat-icon"),y(11,"format_align_left"),g()(),m(12,"mat-button-toggle",22)(13,"mat-icon"),y(14,"format_align_center"),g()(),m(15,"mat-button-toggle",23)(16,"mat-icon"),y(17,"format_align_right"),g()()(),m(18,"p",24),y(19,"Alinhamento vertical do texto"),g(),m(20,"mat-button-toggle-group",25),D("change",function(r){J(e);let o=C(2);return ee(o.onVerticalAlignChange(r.value))}),m(21,"mat-button-toggle",26)(22,"mat-icon"),y(23,"vertical_align_top"),g()(),m(24,"mat-button-toggle",27)(25,"mat-icon"),y(26,"vertical_align_center"),g()(),m(27,"mat-button-toggle",28)(28,"mat-icon"),y(29,"vertical_align_bottom"),g()()(),m(30,"mat-form-field",16)(31,"mat-label"),y(32,"Girar (graus)"),g(),m(33,"mat-select",29),D("selectionChange",function(r){J(e);let o=C(2);return ee(o.onRotationChange(r.value))}),m(34,"mat-option",30),y(35,"0\xB0 (normal)"),g(),m(36,"mat-option",30),y(37,"90\xB0 (vertical, lendo de cima pra baixo)"),g(),m(38,"mat-option",30),y(39,"180\xB0 (de cabe\xE7a pra baixo)"),g(),m(40,"mat-option",30),y(41,"270\xB0 (vertical, lendo de baixo pra cima)"),g()()()}if(t&2){let e=n;_(3),L("value",e.fontSizeDots),_(),L("checked",e.bold),_(4),L("value",e.align),_(12),L("value",e.verticalAlign),_(13),L("value",e.rotationDeg),_(),L("value",0),_(2),L("value",90),_(2),L("value",180),_(2),L("value",270)}}function nV(t,n){t&1&&(m(0,"p",4),y(1,"Imagem selecionada."),g())}function iV(t,n){if(t&1){let e=Le();m(0,"div",0),P(1,eV,4,0,"button",1),m(2,"button",2),D("click",function(){J(e);let r=C();return ee(r.onDuplicate())}),m(3,"mat-icon"),y(4,"content_copy"),g(),y(5," Duplicar "),g(),m(6,"button",3),D("click",function(){J(e);let r=C();return ee(r.onDelete())}),m(7,"mat-icon"),y(8,"delete"),g(),y(9," Remover "),g()(),m(10,"p",4),y(11," Atalhos: duplo clique na caixa tamb\xE9m edita. Ctrl+C copia, Ctrl+V cola, Delete remove. "),g(),ie(12,"mat-divider"),P(13,tV,42,9)(14,nV,2,0,"p",4),ie(15,"mat-divider"),m(16,"h3"),y(17,"Posi\xE7\xE3o e tamanho"),g(),m(18,"p",5),y(19,"Empurrar a caixa \xB7 1 dot por toque"),g(),m(20,"div",6)(21,"button",7),D("click",function(){J(e);let r=C();return ee(r.onNudge(-1,0))}),m(22,"mat-icon"),y(23,"arrow_back"),g()(),m(24,"button",8),D("click",function(){J(e);let r=C();return ee(r.onNudge(0,-1))}),m(25,"mat-icon"),y(26,"arrow_upward"),g()(),m(27,"button",9),D("click",function(){J(e);let r=C();return ee(r.onNudge(0,1))}),m(28,"mat-icon"),y(29,"arrow_downward"),g()(),m(30,"button",10),D("click",function(){J(e);let r=C();return ee(r.onNudge(1,0))}),m(31,"mat-icon"),y(32,"arrow_forward"),g()()(),m(33,"p",4),y(34," Digitar direto resolve o caso da caixa que sumiu da \xE1rea vis\xEDvel: ponha 0 e 0 que ela volta. "),g(),m(35,"div",11)(36,"mat-form-field",12)(37,"mat-label"),y(38,"X (dots)"),g(),m(39,"input",13),D("input",function(r){J(e);let o=C();return ee(o.onXChange(r.target.valueAsNumber))}),g()(),m(40,"mat-form-field",12)(41,"mat-label"),y(42,"Y (dots)"),g(),m(43,"input",13),D("input",function(r){J(e);let o=C();return ee(o.onYChange(r.target.valueAsNumber))}),g()()(),m(44,"div",11)(45,"mat-form-field",12)(46,"mat-label"),y(47,"Largura (dots)"),g(),m(48,"input",14),D("input",function(r){J(e);let o=C();return ee(o.onWidthChange(r.target.valueAsNumber))})("blur",function(){J(e);let r=C();return ee(r.onSizeCommit())})("keydown.enter",function(){J(e);let r=C();return ee(r.onSizeCommit())}),g()(),m(49,"mat-form-field",12)(50,"mat-label"),y(51,"Altura (dots)"),g(),m(52,"input",14),D("input",function(r){J(e);let o=C();return ee(o.onHeightChange(r.target.valueAsNumber))})("blur",function(){J(e);let r=C();return ee(r.onSizeCommit())})("keydown.enter",function(){J(e);let r=C();return ee(r.onSizeCommit())}),g()()()}if(t&2){let e,i=n,r=C();_(),F(r.selectedTextElement()?1:-1),_(12),F((e=r.selectedTextElement())?13:14,e),_(26),L("value",i.xDots),_(4),L("value",i.yDots),_(5),L("value",i.widthDots),_(4),L("value",i.heightDots)}}var Gs=class t{store=u(An);selectedElement=K(()=>{let n=this.store.selectedId();return this.store.layout().elements.find(e=>e.id===n)??null});selectedTextElement=K(()=>{let n=this.selectedElement();return n?.kind==="text"?n:null});onEditText(){let n=this.selectedTextElement();n&&this.store.startEditing(n.id)}onDuplicate(){this.store.duplicateSelected()}onDelete(){let n=this.store.selectedId();n&&this.store.removeElement(n)}onNudge(n,e){let i=this.store.selectedId();i&&this.store.nudgeElement(i,n,e)}onXChange(n){let e=this.selectedElement();e&&Number.isFinite(n)&&this.store.updateElement(e.id,{xDots:Math.round(n)})}onYChange(n){let e=this.selectedElement();e&&Number.isFinite(n)&&this.store.updateElement(e.id,{yDots:Math.round(n)})}onWidthChange(n){let e=this.selectedElement();e&&Number.isFinite(n)&&this.store.updateElement(e.id,{widthDots:Math.round(n)})}onHeightChange(n){let e=this.selectedElement();e&&Number.isFinite(n)&&this.store.updateElement(e.id,{heightDots:Math.round(n)})}onSizeCommit(){let n=this.selectedElement();n&&this.store.resizeElement(n.id,n.widthDots,n.heightDots)}onFontSizeChange(n){let e=this.store.selectedId();e&&Number.isFinite(n)&&this.store.setTextFontSize(e,n)}onBoldChange(n){let e=this.store.selectedId();e&&this.store.setTextBold(e,n)}onAlignChange(n){let e=this.store.selectedId();e&&this.store.updateElement(e,{align:n})}onVerticalAlignChange(n){let e=this.store.selectedId();e&&this.store.updateElement(e,{verticalAlign:n})}onRotationChange(n){let e=this.store.selectedId();e&&this.store.setTextRotation(e,n)}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=T({type:t,selectors:[["app-label-properties-panel"]],decls:1,vars:1,consts:[[1,"properties-actions"],["mat-flat-button","","type","button"],["mat-stroked-button","","type","button",3,"click"],["mat-stroked-button","","color","warn","type","button",3,"click"],[1,"properties-hint"],[1,"properties-label"],[1,"nudge-pad"],["mat-stroked-button","","type","button","aria-label","Empurrar pra esquerda",3,"click"],["mat-stroked-button","","type","button","aria-label","Empurrar pra cima",3,"click"],["mat-stroked-button","","type","button","aria-label","Empurrar pra baixo",3,"click"],["mat-stroked-button","","type","button","aria-label","Empurrar pra direita",3,"click"],[1,"size-fields"],["appearance","outline"],["matInput","","type","number",3,"input","value"],["matInput","","type","number","min","8",3,"input","blur","keydown.enter","value"],["mat-flat-button","","type","button",3,"click"],["appearance","outline",1,"full-width"],["matInput","","type","number","min","8","max","999",3,"input","value"],[3,"change","checked"],["id","rotulo-alinhamento-h",1,"properties-label"],["hideSingleSelectionIndicator","","aria-labelledby","rotulo-alinhamento-h",1,"align-toggle",3,"change","value"],["value","left","aria-label","Alinhar \xE0 esquerda"],["value","center","aria-label","Centralizar"],["value","right","aria-label","Alinhar \xE0 direita"],["id","rotulo-alinhamento-v",1,"properties-label"],["hideSingleSelectionIndicator","","aria-labelledby","rotulo-alinhamento-v",1,"align-toggle",3,"change","value"],["value","top","aria-label","Alinhar ao topo"],["value","middle","aria-label","Centralizar na vertical"],["value","bottom","aria-label","Alinhar \xE0 base"],[3,"selectionChange","value"],[3,"value"]],template:function(e,i){if(e&1&&P(0,iV,53,6),e&2){let r;F((r=i.selectedElement())?0:-1,r)}},dependencies:[Nn,cr,qx,Gv,Hf,Zx,Yv,ff,uf,kn,dr,xi,un,dn,Pf,Of,kf,Nf,mr],styles:["[_nghost-%COMP%]{display:flex;flex-direction:column;gap:.5rem}h3[_ngcontent-%COMP%]{margin:0 0 .25rem;font-size:.9375rem}.full-width[_ngcontent-%COMP%]{width:100%}.properties-hint[_ngcontent-%COMP%]{font-size:.8125rem;color:var(--%NS%mat-sys-on-surface-variant, #616161);text-align:center;margin:0}.properties-label[_ngcontent-%COMP%]{font-size:.75rem;font-weight:500;color:var(--%NS%mat-sys-on-surface-variant, #616161);margin:.5rem 0 .125rem}.properties-actions[_ngcontent-%COMP%]{display:flex;flex-wrap:wrap;gap:.5rem}.properties-actions[_ngcontent-%COMP%]   button[_ngcontent-%COMP%]{flex:1 1 auto;min-width:7rem}.align-toggle[_ngcontent-%COMP%]{display:flex}.align-toggle[_ngcontent-%COMP%]     .mat-button-toggle{flex:1}.nudge-pad[_ngcontent-%COMP%]{display:flex;gap:.375rem}.nudge-pad[_ngcontent-%COMP%]   button[_ngcontent-%COMP%]{flex:1;min-width:0;padding:0}.size-fields[_ngcontent-%COMP%]{display:flex;gap:.5rem}.size-fields[_ngcontent-%COMP%]   mat-form-field[_ngcontent-%COMP%]{flex:1;min-width:0}"]})};var Uf=class t{store=u(An);sheetRef=u(Ts);data=u(mv);host=u(O);title=K(()=>{let n=this.store.selectedId(),e=this.store.layout().elements.find(r=>r.id===n);if(!e)return"Propriedades";if(e.kind==="image")return"Imagem";let i=e.text.trim();return i||"Texto vazio"});constructor(){Kr(n=>{let i=this.host.nativeElement.closest("mat-bottom-sheet-container")??this.host.nativeElement,r=()=>this.data.onHeightChange(i.getBoundingClientRect().height),o=new ResizeObserver(r);o.observe(i),r(),n(()=>{o.disconnect(),this.data.onHeightChange(0)})})}close(){this.store.select(null),this.sheetRef.dismiss()}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=T({type:t,selectors:[["app-label-properties-sheet"]],decls:7,vars:1,consts:[[1,"sheet-head"],[1,"sheet-title"],["mat-icon-button","","type","button","aria-label","Fechar propriedades",3,"click"]],template:function(e,i){e&1&&(m(0,"header",0)(1,"span",1),y(2),g(),m(3,"button",2),D("click",function(){return i.close()}),m(4,"mat-icon"),y(5,"close"),g()()(),ie(6,"app-label-properties-panel")),e&2&&(_(2),ye(i.title()))},dependencies:[Nn,Co,un,dn,Gs],styles:["[_nghost-%COMP%]{display:block}.sheet-head[_ngcontent-%COMP%]{display:flex;align-items:center;justify-content:space-between;gap:.5rem;margin-bottom:.25rem}.sheet-title[_ngcontent-%COMP%]{font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}"]})};var rV=["stageContainer"],Kx=(t,n)=>n.key,zf=(t,n)=>n.id;function oV(t,n){if(t&1&&(m(0,"header",7),ie(1,"app-printer-status-badge",29),m(2,"button",30),y(3),m(4,"mat-icon",31),y(5,"expand_more"),g()()()),t&2){let e=C(),i=ht(8);_(2),L("matMenuTriggerFor",i),_(),At(" ",e.store.layout().sizePreset.label," ")}}function sV(t,n){if(t&1&&(m(0,"mat-option",36),y(1),g()),t&2){let e=n.$implicit;L("value",e.key),_(),ye(e.label)}}function aV(t,n){if(t&1){let e=Le();m(0,"header",32)(1,"h1"),y(2,"Editor de etiquetas"),g(),ie(3,"app-printer-status-badge"),g(),m(4,"div",33)(5,"mat-form-field",34)(6,"mat-label"),y(7,"Tamanho da etiqueta"),g(),m(8,"mat-select",35),D("selectionChange",function(r){J(e);let o=C();return ee(o.onSizePresetChange(r.value))}),Jt(9,sV,2,2,"mat-option",36,Kx),g()(),m(11,"button",37),D("click",function(){J(e);let r=C();return ee(r.onAddText())}),m(12,"mat-icon"),y(13,"title"),g(),y(14," Adicionar texto "),g(),m(15,"button",37),D("click",function(){J(e),C();let r=ht(4);return ee(r.click())}),m(16,"mat-icon"),y(17,"image"),g(),y(18," Adicionar imagem "),g(),m(19,"button",38)(20,"mat-icon"),y(21,"folder"),g(),y(22," Etiquetas salvas "),g(),m(23,"button",39),D("click",function(){J(e);let r=C();return ee(r.onPrint())}),m(24,"mat-icon"),y(25,"print"),g(),y(26),g()()}if(t&2){let e=C(),i=ht(17);_(8),L("value",e.store.layout().sizePreset.key),_(),en(e.sizePresets),_(10),L("matMenuTriggerFor",i),_(4),L("disabled",e.printing()),_(3),At(" ",e.printing()?"Imprimindo\u2026":"Imprimir"," ")}}function lV(t,n){if(t&1){let e=Le();m(0,"button",12),D("click",function(){let r=J(e).$implicit,o=C();return ee(o.onSizePresetChange(r.key))}),y(1),g()}if(t&2){let e=n.$implicit;_(),At(" ",e.label," ")}}function cV(t,n){t&1&&(m(0,"button",11),y(1,"Nenhum elemento ainda"),g())}function dV(t,n){if(t&1){let e=Le();m(0,"button",12),D("click",function(){let r=J(e).$implicit,o=C();return ee(o.store.select(r.id))}),m(1,"mat-icon"),y(2),g(),y(3),g()}if(t&2){let e=n.$implicit,i=C();_(2),ye(e.kind==="text"?"title":"image"),_(),At(" ",i.elementPreviewLabel(e)," ")}}function uV(t,n){if(t&1){let e=Le();m(0,"div",40)(1,"button",41),D("click",function(){let r=J(e).$implicit,o=C(2);return ee(o.onLoadFromLibrary(r.id))}),m(2,"span",42),y(3),g()(),m(4,"button",43),D("click",function(r){let o=J(e).$implicit,s=C(2);return ee(s.onDeleteFromLibrary(o.id,o.name,r))}),m(5,"mat-icon"),y(6,"delete"),g()()()}if(t&2){let e=n.$implicit;_(3),ye(e.name),_(),re("aria-label","Remover etiqueta salva "+e.name)}}function fV(t,n){if(t&1&&(ie(0,"mat-divider"),Jt(1,uV,7,2,"div",40,zf)),t&2){let e=C();_(),en(e.library.savedLabels())}}function hV(t,n){if(t&1&&ie(0,"app-label-element-box",18),t&2){let e=n.$implicit,i=C();L("element",e)("scale",i.scale())("labelWidthDots",i.stageWidthDots())("labelHeightDots",i.stageHeightDots())}}function mV(t,n){if(t&1&&ie(0,"div",44),t&2){let e=n.$implicit,i=C();Ut("left",e*i.scale(),"px")}}function pV(t,n){if(t&1&&ie(0,"div",45),t&2){let e=n.$implicit,i=C();Ut("top",e*i.scale(),"px")}}function gV(t,n){t&1&&(m(0,"p",21),y(1," Etiqueta vazia. Comece por "),m(2,"strong"),y(3,"Adicionar texto"),g(),y(4,". "),g())}function vV(t,n){t&1&&(m(0,"p",26),y(1," Arraste pra posicionar. As caixas grudam nas bordas/centro de outras caixas e da pr\xF3pria etiqueta. "),g())}function bV(t,n){if(t&1){let e=Le();m(0,"button",50),D("click",function(){let r=J(e).$implicit,o=C(3);return ee(o.store.select(r.id))}),m(1,"mat-icon"),y(2),g(),m(3,"span",51),y(4),g()()}if(t&2){let e=n.$implicit,i=C(3);B("is-selected",i.store.selectedId()===e.id),_(2),ye(e.kind==="text"?"title":"image"),_(2),ye(i.elementPreviewLabel(e))}}function _V(t,n){if(t&1&&(m(0,"mat-card",46)(1,"mat-card-content")(2,"h2"),y(3,"Elementos na etiqueta"),g(),m(4,"p",48),y(5," Selecione por aqui quando uma caixa ficar dif\xEDcil de achar ou clicar na etiqueta (muito pequena, vazia ou fora da \xE1rea vis\xEDvel). "),g(),Jt(6,bV,5,4,"button",49,zf),g()()),t&2){let e=C(2);_(6),en(e.store.layout().elements)}}function yV(t,n){t&1&&(m(0,"mat-card",47)(1,"mat-card-content")(2,"h2"),y(3,"Propriedades"),g(),ie(4,"app-label-properties-panel"),g()())}function SV(t,n){if(t&1&&(m(0,"div",27),P(1,_V,8,0,"mat-card",46),P(2,yV,5,0,"mat-card",47),g()),t&2){let e=C();_(),F(e.store.layout().elements.length>0?1:-1),_(),F(e.hasSelection()?2:-1)}}function CV(t,n){if(t&1){let e=Le();m(0,"nav",28)(1,"button",52),D("click",function(){J(e);let r=C();return ee(r.onAddText())}),m(2,"mat-icon"),y(3,"title"),g(),m(4,"span"),y(5,"Texto"),g()(),m(6,"button",52),D("click",function(){J(e),C();let r=ht(4);return ee(r.click())}),m(7,"mat-icon"),y(8,"image"),g(),m(9,"span"),y(10,"Imagem"),g()(),m(11,"button",53)(12,"mat-icon"),y(13,"layers"),g(),m(14,"span"),y(15,"Elementos"),g()(),m(16,"button",53)(17,"mat-icon"),y(18,"folder"),g(),m(19,"span"),y(20,"Salvas"),g()(),m(21,"button",54),D("click",function(){J(e);let r=C();return ee(r.onPrint())}),m(22,"mat-icon"),y(23,"print"),g(),y(24),g()()}if(t&2){let e=C(),i=ht(12),r=ht(17);_(11),L("matMenuTriggerFor",i),_(5),L("matMenuTriggerFor",r),_(5),L("disabled",e.printing()),_(3),At(" ",e.printing()?"Imprimindo\u2026":"Imprimir"," ")}}var Xx=24,DV=4,EV="(max-width: 900px)",$f=class t{store=u(An);library=u(Af);printLabelLayout=u(Rf);printerConnection=u(pr);snackBar=u(xx);bottomSheet=u(QD);dialog=u(Rs);injector=u(R);destroyRef=u(qe);sizePresets=bl;printing=U(!1);MIN_ZOOM=1;MAX_ZOOM=4;stageContainerRef=ui("stageContainer");containerWidthPx=U(400);containerHeightBudgetPx=U(400);resizeObserver=null;isCompact=U(!1);sheetHeightPx=U(0);zoom=U(1);sheetRef=null;stageWidthDots=K(()=>vi(this.store.layout().sizePreset));stageHeightDots=K(()=>bi(this.store.layout().sizePreset));scale=K(()=>{let n=Math.max(50,this.containerWidthPx()-Xx),e=Math.max(50,this.containerHeightBudgetPx()-Xx),i=Math.min(n/this.stageWidthDots(),e/this.stageHeightDots());return Math.min(i,DV)*this.zoom()});stageWidthPx=K(()=>this.stageWidthDots()*this.scale());stageHeightPx=K(()=>this.stageHeightDots()*this.scale());zoomLabel=K(()=>`${Math.round(this.zoom()*100)}%`);hasSelection=K(()=>this.store.selectedId()!==null);constructor(){this.watchCompactQuery(),Kr(n=>{let e=this.stageContainerRef()?.nativeElement;e&&(this.resizeObserver=new ResizeObserver(()=>this.measureStageContainer()),this.resizeObserver.observe(e),this.measureStageContainer(),n(()=>this.resizeObserver?.disconnect()))}),ut(()=>{let n=this.isCompact()&&this.hasSelection();Ne(()=>n?this.openPropertiesSheet():this.closePropertiesSheet())})}watchCompactQuery(){if(typeof window>"u"||!window.matchMedia)return;let n=window.matchMedia(EV);this.isCompact.set(n.matches);let e=i=>this.isCompact.set(i.matches);n.addEventListener("change",e),this.destroyRef.onDestroy(()=>n.removeEventListener("change",e))}openPropertiesSheet(){if(this.sheetRef)return;let n={onHeightChange:e=>this.sheetHeightPx.set(e)};this.sheetRef=this.bottomSheet.open(Uf,{hasBackdrop:!1,panelClass:"properties-bottom-sheet",injector:this.injector,data:n}),this.sheetRef.afterDismissed().subscribe(()=>{this.sheetRef=null,this.sheetHeightPx.set(0),this.store.select(null)})}closePropertiesSheet(){this.sheetRef?.dismiss()}measureStageContainer(){let n=this.stageContainerRef()?.nativeElement;if(!n)return;let e=n.getBoundingClientRect();this.containerWidthPx.set(e.width),e.height>=1&&this.containerHeightBudgetPx.set(e.height)}onZoom(n){let e=Math.round((this.zoom()+n*.25)*100)/100;this.zoom.set(Math.min(this.MAX_ZOOM,Math.max(this.MIN_ZOOM,e)))}onResetZoom(){this.zoom.set(1)}onSizePresetChange(n){this.store.setSizePresetKey(n)}onStageBackgroundClick(){this.store.select(null)}elementPreviewLabel(n){if(n.kind==="image")return"Imagem";let e=n.text.trim();return e||"Texto vazio"}onAddText(){this.store.addTextElement()}onImageSelected(n){let e=n.target,i=e.files?.[0];if(!i)return;let r=new FileReader;r.onload=()=>{let o=r.result,s=new Image;s.onload=()=>{this.store.addImageElement(o,s.naturalWidth,s.naturalHeight)},s.src=o},r.readAsDataURL(i),e.value=""}onKeyDown(n){let e=n.target;if(e.tagName==="TEXTAREA"||e.tagName==="INPUT")return;let i=this.store.selectedId();if(n.key==="Delete"||n.key==="Backspace"){i&&(this.store.removeElement(i),n.preventDefault());return}if(i&&n.key.startsWith("Arrow")){let o=n.shiftKey?10:1,s=n.key==="ArrowLeft"?-o:n.key==="ArrowRight"?o:0,a=n.key==="ArrowUp"?-o:n.key==="ArrowDown"?o:0;(s||a)&&(this.store.nudgeElement(i,s,a),n.preventDefault());return}if(!(n.ctrlKey||n.metaKey))return;let r=n.key.toLowerCase();r==="c"&&i?(this.store.copySelected(),n.preventDefault()):r==="v"&&this.store.hasClipboard()&&(this.store.pasteClipboard(),n.preventDefault())}async onPrint(){if(this.store.layout().elements.length===0){this.snackBar.open("Adicione algo na etiqueta antes de imprimir.","Fechar",{duration:3e3});return}let n=this.printerConnection.device();if(this.printerConnection.status()!=="connected"||!n){this.snackBar.open("Nenhuma impressora alcan\xE7\xE1vel neste endere\xE7o.","Configurar",{duration:6e3}).onAction().subscribe(()=>this.dialog.open($s));return}this.printing.set(!0);try{await this.printLabelLayout.execute(this.store.layout(),n),this.snackBar.open("Etiqueta enviada para a impressora.","Fechar",{duration:3e3})}catch(e){let i=e instanceof Error?e.message:"Falha ao imprimir.";this.snackBar.open(i,"Fechar",{duration:5e3})}finally{this.printing.set(!1)}}onSaveToLibrary(){let n=this.store.layout();if(n.elements.length===0){this.snackBar.open("Adicione algo na etiqueta antes de salvar.","Fechar",{duration:3e3});return}let e=window.prompt("Nome da etiqueta:","");if(!e)return;let i=this.library.save(e,n);this.snackBar.open(i?`Etiqueta "${e}" salva.`:"N\xE3o foi poss\xEDvel salvar (armazenamento local cheio ou indispon\xEDvel).","Fechar",{duration:3e3})}onLoadFromLibrary(n){let e=this.library.savedLabels().find(i=>i.id===n);e&&this.store.loadLayout(this.library.toLayout(e))}onDeleteFromLibrary(n,e,i){i.stopPropagation(),window.confirm(`Remover a etiqueta salva "${e}"?`)&&this.library.remove(n)}onExportLibrary(){if(this.library.savedLabels().length===0){this.snackBar.open("Nenhuma etiqueta salva pra exportar.","Fechar",{duration:3e3});return}this.library.exportAll()}async onImportLibraryFile(n){let e=n.target,i=e.files?.[0];if(i)try{let r=await this.library.importFromFile(i);this.snackBar.open(`${r} etiqueta(s) importada(s).`,"Fechar",{duration:3e3})}catch(r){let o=r instanceof Xl?r.message:"Falha ao importar o arquivo.";this.snackBar.open(o,"Fechar",{duration:4e3})}finally{e.value=""}}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=T({type:t,selectors:[["app-label-designer-page"]],viewQuery:function(e,i){e&1&&Wi(i.stageContainerRef,rV,5),e&2&&qi()},hostBindings:function(e,i){e&1&&D("keydown",function(o){return i.onKeyDown(o)},vp)},features:[Be([An])],decls:57,vars:19,consts:[["imageInput",""],["importInput",""],["sizeMenu","matMenu"],["elementsMenu","matMenu"],["libraryMenu","matMenu"],["stageContainer",""],[1,"designer-layout"],[1,"compact-bar"],["type","file","accept","image/*","hidden","",3,"change"],["type","file","accept","application/json","hidden","",3,"change"],["mat-menu-item","","type","button"],["mat-menu-item","","type","button","disabled",""],["mat-menu-item","","type","button",3,"click"],[1,"designer-columns"],[1,"stage-card"],[1,"stage-card-content"],[1,"stage-container"],[1,"stage",3,"pointerdown"],[3,"element","scale","labelWidthDots","labelHeightDots"],[1,"guide","guide-vertical",3,"left"],[1,"guide","guide-horizontal",3,"top"],[1,"stage-empty"],[1,"zoom-controls"],["mat-icon-button","","type","button","aria-label","Diminuir o zoom",3,"click","disabled"],["mat-button","","type","button",1,"zoom-value",3,"click","disabled"],["mat-icon-button","","type","button","aria-label","Aumentar o zoom",3,"click","disabled"],[1,"stage-hint"],[1,"side-panels"],["aria-label","A\xE7\xF5es da etiqueta",1,"action-bar"],["compact",""],["mat-stroked-button","","type","button",1,"size-pill",3,"matMenuTriggerFor"],["iconPositionEnd",""],[1,"designer-header"],[1,"toolbar"],["appearance","outline",1,"size-field"],[3,"selectionChange","value"],[3,"value"],["mat-stroked-button","","type","button",3,"click"],["mat-stroked-button","","type","button",3,"matMenuTriggerFor"],["mat-flat-button","","color","primary","type","button",1,"print-button",3,"click","disabled"],[1,"saved-label-row"],["mat-menu-item","","type","button",1,"saved-label-item",3,"click"],[1,"saved-label-name"],["mat-menu-item","","type","button",1,"saved-label-delete",3,"click"],[1,"guide","guide-vertical"],[1,"guide","guide-horizontal"],[1,"elements-card"],[1,"properties-card"],[1,"properties-hint"],["type","button",1,"element-list-item",3,"is-selected"],["type","button",1,"element-list-item",3,"click"],[1,"element-list-label"],["mat-button","","type","button",1,"action",3,"click"],["mat-button","","type","button",1,"action",3,"matMenuTriggerFor"],["mat-flat-button","","color","primary","type","button",1,"print-fab",3,"click","disabled"]],template:function(e,i){if(e&1){let r=Le();m(0,"div",6),P(1,oV,6,2,"header",7)(2,aV,27,4),m(3,"input",8,0),D("change",function(s){return i.onImageSelected(s)}),g(),m(5,"input",9,1),D("change",function(s){return i.onImportLibraryFile(s)}),g(),m(7,"mat-menu",null,2),Jt(9,lV,2,1,"button",10,Kx),g(),m(11,"mat-menu",null,3),P(13,cV,2,0,"button",11),Jt(14,dV,4,2,"button",10,zf),g(),m(16,"mat-menu",null,4)(18,"button",12),D("click",function(){return i.onSaveToLibrary()}),m(19,"mat-icon"),y(20,"save"),g(),y(21," Salvar etiqueta atual\u2026 "),g(),P(22,fV,3,0),ie(23,"mat-divider"),m(24,"button",12),D("click",function(){return i.onExportLibrary()}),m(25,"mat-icon"),y(26,"download"),g(),y(27," Exportar tudo (.json) "),g(),m(28,"button",12),D("click",function(){J(r);let s=ht(6);return ee(s.click())}),m(29,"mat-icon"),y(30,"upload"),g(),y(31," Importar (.json) "),g()(),m(32,"div",13)(33,"mat-card",14)(34,"mat-card-content",15)(35,"div",16,5)(37,"div",17),D("pointerdown",function(){return i.onStageBackgroundClick()}),Jt(38,hV,1,4,"app-label-element-box",18,zf),Jt(40,mV,1,2,"div",19,Jd),Jt(42,pV,1,2,"div",20,Jd),g(),P(44,gV,5,0,"p",21),g(),m(45,"div",22)(46,"button",23),D("click",function(){return i.onZoom(-1)}),m(47,"mat-icon"),y(48,"remove"),g()(),m(49,"button",24),D("click",function(){return i.onResetZoom()}),y(50),g(),m(51,"button",25),D("click",function(){return i.onZoom(1)}),m(52,"mat-icon"),y(53,"add"),g()()(),P(54,vV,2,0,"p",26),g()(),P(55,SV,3,2,"div",27),g(),P(56,CV,25,4,"nav",28),g()}e&2&&(Ut("--%NS%sheet-height",i.sheetHeightPx(),"px"),B("is-compact",i.isCompact()),_(),F(i.isCompact()?1:2),_(8),en(i.sizePresets),_(4),F(i.store.layout().elements.length===0?13:-1),_(),en(i.store.layout().elements),_(8),F(i.library.savedLabels().length>0?22:-1),_(15),Ut("width",i.stageWidthPx(),"px")("height",i.stageHeightPx(),"px"),_(),en(i.store.layout().elements),_(2),en(i.store.activeGuides().vertical),_(2),en(i.store.activeGuides().horizontal),_(2),F(i.store.layout().elements.length===0?44:-1),_(2),L("disabled",i.zoom()<=i.MIN_ZOOM),_(3),L("disabled",i.zoom()===1),_(),At(" ",i.zoomLabel()," "),_(),L("disabled",i.zoom()>=i.MAX_ZOOM),_(3),F(i.isCompact()?-1:54),_(),F(i.isCompact()?-1:55),_(),F(i.isCompact()?56:-1))},dependencies:[Nn,cr,Co,pE,hE,mE,ff,uf,kn,dr,xi,un,dn,WE,Fs,Bl,GE,kf,Nf,mr,Bf,Gs,Lf],styles:["[_nghost-%COMP%]{display:flex;flex-direction:column;flex:1 1 auto;min-height:0}.designer-layout[_ngcontent-%COMP%]{display:flex;flex-direction:column;flex:1 1 auto;min-height:0;gap:1.5rem;padding:1.5rem;max-width:1800px;width:100%;margin:0 auto;box-sizing:border-box}@media(max-width:900px){.designer-layout[_ngcontent-%COMP%]{padding:.75rem;gap:.75rem}}@media(max-height:500px)and (orientation:landscape){.designer-layout[_ngcontent-%COMP%]{padding:.5rem .75rem;gap:.5rem}}.designer-header[_ngcontent-%COMP%]{flex:0 0 auto;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:1rem}.designer-header[_ngcontent-%COMP%]   h1[_ngcontent-%COMP%]{margin:0;font-size:1.5rem}@media(max-width:900px){.designer-header[_ngcontent-%COMP%]   h1[_ngcontent-%COMP%]{font-size:1.25rem}}@media(max-height:500px)and (orientation:landscape){.designer-header[_ngcontent-%COMP%]   h1[_ngcontent-%COMP%]{font-size:1.0625rem}}.toolbar[_ngcontent-%COMP%]{flex:0 0 auto;display:flex;align-items:flex-start;flex-wrap:wrap;gap:.75rem}.size-field[_ngcontent-%COMP%]{min-width:220px}.print-button[_ngcontent-%COMP%]{margin-left:auto}.designer-columns[_ngcontent-%COMP%]{display:flex;flex:1 1 auto;min-height:0;gap:1.5rem}@media(max-width:900px){.designer-columns[_ngcontent-%COMP%]{flex-direction:column}}.stage-card[_ngcontent-%COMP%]{display:flex;flex-direction:column;flex:1 1 0;min-width:0;min-height:0}@media(max-width:900px){.stage-card[_ngcontent-%COMP%]{min-height:min(45dvh,420px)}}@media(max-height:500px)and (orientation:landscape){.stage-card[_ngcontent-%COMP%]{min-height:60dvh}}.stage-card-content[_ngcontent-%COMP%]{display:flex;flex-direction:column;align-items:center;flex:1 1 auto;min-height:0;position:relative}.compact-bar[_ngcontent-%COMP%]{flex:0 0 auto;display:flex;align-items:center;gap:.5rem;min-width:0}.compact-bar[_ngcontent-%COMP%]   app-printer-status-badge[_ngcontent-%COMP%]{min-width:0;flex:1 1 auto}.size-pill[_ngcontent-%COMP%]{flex:0 0 auto;white-space:nowrap}.action-bar[_ngcontent-%COMP%]{flex:0 0 auto;display:flex;align-items:center;gap:.25rem}.action-bar[_ngcontent-%COMP%]   .action[_ngcontent-%COMP%]{flex:1 1 0;min-width:0;display:inline-flex;flex-direction:column;align-items:center;justify-content:center;gap:.125rem;min-height:52px;padding:0 .125rem;line-height:1.1}.action-bar[_ngcontent-%COMP%]   .action[_ngcontent-%COMP%]   span[_ngcontent-%COMP%]{font-size:.6875rem}.action-bar[_ngcontent-%COMP%]   .action[_ngcontent-%COMP%]     .mat-mdc-button-touch-target{height:100%}.action-bar[_ngcontent-%COMP%]   .print-fab[_ngcontent-%COMP%]{flex:0 0 auto;min-height:52px;border-radius:26px;padding:0 1rem}.zoom-controls[_ngcontent-%COMP%]{position:absolute;top:.5rem;right:.5rem;z-index:5;display:flex;align-items:center;gap:.125rem;padding:0 .25rem;border-radius:20px;background:var(--%NS%mat-sys-surface-container-high, #fff);box-shadow:0 1px 4px #0003}.zoom-controls[_ngcontent-%COMP%]   .zoom-value[_ngcontent-%COMP%]{min-width:3.5rem;font-variant-numeric:tabular-nums}.stage-empty[_ngcontent-%COMP%]{position:absolute;left:50%;top:50%;transform:translate(-50%,-50%);margin:0;padding:0 1rem;text-align:center;font-size:.875rem;color:var(--%NS%mat-sys-on-surface-variant, #616161);pointer-events:none}.stage-container[_ngcontent-%COMP%]{width:100%;display:flex;justify-content:center;align-items:center;position:relative;margin-bottom:var(--%NS%sheet-height, 0px);transition:margin-bottom .18s ease;overflow:auto}@media(prefers-reduced-motion:reduce){.stage-container[_ngcontent-%COMP%]{transition:none}}.stage-container[_ngcontent-%COMP%]{flex:1 1 auto;min-height:0}.stage[_ngcontent-%COMP%]{position:relative;flex:0 0 auto;margin:auto;background:#fff;border:1px solid var(--%NS%mat-sys-outline, #9e9e9e);overflow:visible}.stage-hint[_ngcontent-%COMP%], .properties-hint[_ngcontent-%COMP%]{font-size:.8125rem;color:var(--%NS%mat-sys-on-surface-variant, #616161);text-align:center}.guide[_ngcontent-%COMP%]{position:absolute;background:#e91e63;pointer-events:none;z-index:10}.guide-vertical[_ngcontent-%COMP%]{top:0;bottom:0;width:1px}.guide-horizontal[_ngcontent-%COMP%]{left:0;right:0;height:1px}.properties-card[_ngcontent-%COMP%]   mat-card-content[_ngcontent-%COMP%], .elements-card[_ngcontent-%COMP%]   mat-card-content[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:.5rem}.side-panels[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:1.5rem;flex:0 1 380px;min-width:320px;min-height:0;overflow-y:auto}@media(max-width:900px){.side-panels[_ngcontent-%COMP%]{min-width:0;overflow-y:visible}}.element-list-item[_ngcontent-%COMP%]{display:flex;align-items:center;gap:.5rem;width:100%;padding:.5rem .75rem;border:1px solid var(--%NS%mat-sys-outline, #9e9e9e);border-radius:4px;background:none;cursor:pointer;font:inherit;text-align:left}.element-list-item.is-selected[_ngcontent-%COMP%]{border-color:#3f51b5;background:color-mix(in srgb,#3f51b5 8%,transparent)}.element-list-label[_ngcontent-%COMP%]{flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.saved-label-row[_ngcontent-%COMP%]{display:flex;align-items:center}.saved-label-item[_ngcontent-%COMP%]{display:flex;align-items:center;gap:.5rem;flex:1;min-width:0}.saved-label-name[_ngcontent-%COMP%]{flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;text-align:left}.saved-label-delete[_ngcontent-%COMP%]{flex:0 0 auto;width:48px;min-width:48px;padding:0;justify-content:center;color:var(--%NS%mat-sys-error, #c62828)}.saved-label-delete[_ngcontent-%COMP%]   .mat-icon[_ngcontent-%COMP%]{margin-right:0}.saved-label-delete[_ngcontent-%COMP%]:hover{opacity:.8}"]})};var Qx=[{path:"",component:$f},{path:"designer",redirectTo:"",pathMatch:"full"}];var Jx={providers:[um(),Ug(Qx),{provide:_s,useClass:zu},{provide:Vu,useClass:Uu}]};var Gf=class t{static \u0275fac=function(e){return new(e||t)};static \u0275cmp=T({type:t,selectors:[["app-root"]],decls:2,vars:0,consts:[[1,"app-content"]],template:function(e,i){e&1&&(m(0,"div",0),ie(1,"router-outlet"),g())},dependencies:[ml],styles:["[_nghost-%COMP%]{display:flex;flex-direction:column;height:100dvh}.app-content[_ngcontent-%COMP%]{display:flex;flex-direction:column;flex:1 1 auto;min-height:0}router-outlet[_ngcontent-%COMP%]{display:none}"]})};sg(Gf,Jx).catch(t=>console.error(t));
