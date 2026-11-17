<!--
調査メモ (英語のまま)。mautrix-go v0.33.0 の source を読んで作った bridgev2 の API 要約。実装では必ず
`go doc` / module cache の source で確かめること (`[INFERENCE]` と書いた箇所は未確認)。
-->

# mautrix-go v0.33.0 bridgev2 — API digest for a new network connector (traQ)

Source: `/tmp/research/mautrix-go` @ `548c01e Bump version to v0.33.0`. Module `maunium.net/go/mautrix`, `go 1.26.0` (+ `toolchain go1.27.4`) in go.mod; the code uses `new(expr)` (e.g. `DefaultChatName = new("")`), so the connector needs Go ≥ 1.26.
Paths are relative to the repo root. Doc comments trimmed to one line. `[INFERENCE]` = not directly verified in this repo's code.
This is API facts only: no code copied from AGPL bridges. mautrix-go itself is MPL-2.0.

---

## 1. Network connector & NetworkAPI

### `bridgev2.NetworkConnector` — `bridgev2/networkinterface.go`
```go
type NetworkConnector interface {
	Init(*Bridge)                          // store bridge; no network/blocking work
	Start(context.Context) error           // non-user-specific startup; logins are loaded separately
	GetName() BridgeName                   // called BEFORE config load too (fills example config); must have defaults
	GetDBMetaTypes() database.MetaTypes    // called before Init; hardcoded
	GetCapabilities() *NetworkGeneralCapabilities
	GetConfig() (example string, data any, upgrader configupgrade.Upgrader)
	LoadUserLogin(ctx context.Context, login *UserLogin) error // fill login.Client; runs under bridge cache lock: NO slow ops
	GetLoginFlows() []LoginFlow
	CreateLogin(ctx context.Context, user *User, flowID string) (LoginProcess, error) // no work; work starts in Start()
	GetBridgeInfoVersion() (info, capabilities int) // bump to resend m.bridge / room capabilities to all rooms
}
```

### Optional connector interfaces (same file)
```go
type StoppableNetwork interface { NetworkConnector; Stop() }                 // after all clients disconnected
type ConfigValidatingNetwork interface { NetworkConnector; ValidateConfig() error } // config already unmarshaled into GetConfig() data
type MaxFileSizeingNetwork interface { NetworkConnector; SetMaxFileSize(maxSize int64) } // async after startup; assume 50 MiB before
type IdentifierValidatingNetwork interface { NetworkConnector; ValidateUserID(id networkid.UserID) bool }
type TransactionIDGeneratingNetwork interface { NetworkConnector; GenerateTransactionID(userID id.UserID, roomID id.RoomID, eventType event.Type) networkid.RawTransactionID }
type PortalBridgeInfoFillingNetwork interface { NetworkConnector; FillPortalBridgeInfo(portal *Portal, content *event.BridgeEventContent) }
type NetworkResettingNetwork interface { NetworkConnector; ResetHTTPTransport(); ResetNetworkConnections() }
type DirectMediableNetwork interface { NetworkConnector; SetUseDirectMedia(); Download(ctx context.Context, mediaID networkid.MediaID, params map[string]string) (mediaproxy.GetMediaResponse, error) }
```
Call sites: `ValidateConfig` in `bridgev2/matrix/mxmain/main.go` `validateConfig()` (Init, before DB/bridge creation); `SetMaxFileSize(br.MediaConfig.UploadSize)` in `bridgev2/matrix/connector.go` after fetching the media config.

### `BridgeName` — `bridgev2/networkinterface.go`
```go
type BridgeName struct {
	DisplayName          string              `json:"displayname"`        // e.g. `Discord`
	NetworkURL           string              `json:"network_url"`
	NetworkIcon          id.ContentURIString `json:"network_icon"`       // mxc://
	NetworkID            string              `json:"network_id"`         // e.g. `discord`; used in username template `$<<.NetworkID>>_{{.}}`
	BeeperBridgeType     string              `json:"beeper_bridge_type"` // Go import path is a good choice
	DefaultPort          uint16              `json:"default_port,omitzero"`           // example-config appservice port, default 8080
	DefaultCommandPrefix string              `json:"default_command_prefix,omitempty"` // defaults to NetworkID; must include "!"
}
```

### `NetworkGeneralCapabilities` — `bridgev2/networkinterface.go`
```go
type NetworkGeneralCapabilities struct {
	DisappearingMessages     bool                     // enables disappearing loop
	AggressiveUpdateInfo     bool                     // re-request user info on incoming messages even if ghost has info
	ImplicitReadReceipts     bool                     // call HandleMatrixReadReceipt with fake data on new Matrix message
	OutgoingMessageTimeouts  *OutgoingTimeoutConfig   // with AddPendingToSave mechanism
	Provisioning             ProvisioningCapabilities
	NetworkIsImmediateParent bool
	TypingRepeatInterval     time.Duration            // re-call HandleMatrixTyping at this interval
}
```

### `NetworkAPI` (per-login client, stored in `UserLogin.Client`) — `bridgev2/networkinterface.go`
```go
type NetworkAPI interface {
	Connect(ctx context.Context)          // no error return: report via UserLogin.BridgeState.Send
	Disconnect()
	IsLoggedIn() bool                     // cached data only, no IO
	LogoutRemote(ctx context.Context)
	IsThisUser(ctx context.Context, userID networkid.UserID) bool
	GetChatInfo(ctx context.Context, portal *Portal) (*ChatInfo, error) // nil fields = unchanged; "" = clear
	GetUserInfo(ctx context.Context, ghost *Ghost) (*UserInfo, error)
	GetCapabilities(ctx context.Context, portal *Portal) *event.RoomFeatures
	HandleMatrixMessage(ctx context.Context, msg *MatrixMessage) (message *MatrixMessageResponse, err error) // non-edit messages only
}
type NetworkAPIWithUserID interface { NetworkAPI; GetUserID() networkid.UserID } // may return "" if unknown
```

### Optional NetworkAPI interfaces we implement
```go
type BackfillingNetworkAPI interface { NetworkAPI
	FetchMessages(ctx context.Context, fetchParams FetchMessagesParams) (*FetchMessagesResponse, error) }
type EditHandlingNetworkAPI interface { NetworkAPI
	HandleMatrixEdit(ctx context.Context, msg *MatrixEdit) error } // bridge saves msg.EditTarget afterwards; you may mutate it
type ReactionHandlingNetworkAPI interface { NetworkAPI
	PreHandleMatrixReaction(ctx context.Context, msg *MatrixReaction) (MatrixReactionPreResponse, error) // returns IDs for dedup
	HandleMatrixReaction(ctx context.Context, msg *MatrixReaction) (reaction *database.Reaction, err error) // returned struct may be empty; bridge fills fields
	HandleMatrixReactionRemove(ctx context.Context, msg *MatrixReactionRemove) error }
type RedactionHandlingNetworkAPI interface { NetworkAPI
	HandleMatrixMessageRemove(ctx context.Context, msg *MatrixMessageRemove) error }
type ReadReceiptHandlingNetworkAPI interface { NetworkAPI
	HandleMatrixReadReceipt(ctx context.Context, msg *MatrixReadReceipt) error } // also for non-bridged targets: ExactMessage may be nil
type ChatViewingNetworkAPI interface { NetworkAPI
	HandleMatrixViewingChat(ctx context.Context, msg *MatrixViewingChat) error }
type TypingHandlingNetworkAPI interface { NetworkAPI; HandleMatrixTyping(ctx context.Context, msg *MatrixTyping) error }
type UserSearchingNetworkAPI interface { IdentifierResolvingNetworkAPI; SearchUsers(ctx context.Context, query string) ([]*ResolveIdentifierResponse, error) }
type IdentifierResolvingNetworkAPI interface { NetworkAPI; ResolveIdentifier(ctx context.Context, identifier string, createChat bool) (*ResolveIdentifierResponse, error) }
```
```go
type MatrixReactionPreResponse struct {
	SenderID     networkid.UserID
	EmojiID      networkid.EmojiID   // dedup key with (message, part, sender); "" = one reaction per user (overrides)
	Emoji        string
	MaxReactions int                 // >0: bridge passes ExistingReactionsToKeep / removes extras
}
type MatrixReadReceipt struct {
	Portal       *Portal
	EventID      id.EventID          // event targeted by the receipt
	ExactMessage *database.Message   // nil if the event isn't a bridged message
	ReadUpTo     time.Time           // msg timestamp if present, else receipt ts
	LastRead     time.Time           // ReadUpTo of the previous receipt
	Receipt      event.ReadReceipt
	Implicit     bool                // triggered by a timeline event (ImplicitReadReceipts)
}
type MatrixViewingChat struct {
	Portal *Portal // nil when user switched to a chat of a different bridge
}
```
**ChatViewing:** the doc comment says it is *never* called by the standard appservice connector (Matrix has no standard "chat open" signal). Only Beeper clients send it, every ~5 s, with no close signal. No call site exists in this repo (grep for `HandleMatrixViewingChat` finds only the interface), so on a standard homeserver it is dead code.

**Read receipts from Matrix:** these need `appservice.ephemeral_events: true` (default `true` in `bridgev2/matrix/mxmain/example-config.yaml`).

### `GetCapabilities(ctx, portal) *event.RoomFeatures` — `event/capabilities.go`
Relevant fields:
```go
type RoomFeatures struct {
	ID            string               `json:"id,omitzero"`        // optional; else hash of contents (GetID)
	Formatting    FormattingFeatureMap `json:"formatting,omitempty"` // map[FormattingFeature]CapabilitySupportLevel
	File          FileFeatureMap       `json:"file,omitempty"`       // map[CapabilityMsgType]*FileFeatures
	MaxTextLength int                  `json:"max_text_length,omitzero"` // NOT enforced by bridge: check in connector after conversion
	LocationMessage, Thread, Reply, Edit, Delete, Reaction CapabilitySupportLevel // json: location_message, thread, reply, edit, delete, reaction
	EditMaxCount int; EditMaxAge *jsontime.Seconds; DeleteForMe bool; DeleteMaxAge *jsontime.Seconds
	ReactionCount int; AllowedReactions []string; CustomEmojiReactions bool
	ReadReceipts, TypingNotifications, Archive, MarkAsUnread, DeleteChat, ... bool
	// also: State, MemberActions, Poll*, DisappearingTimer, MessageRequest, PerMessageProfileRelay
}
type FileFeatures struct {
	MimeTypes        map[string]CapabilitySupportLevel `json:"mime_types"` // keys: exact, "type/*", "*/*"; missing = Rejected
	Caption          CapabilitySupportLevel `json:"caption,omitzero"`
	MaxCaptionLength int                    `json:"max_caption_length,omitzero"`
	MaxSize int64; MaxWidth, MaxHeight int; MaxDuration *jsontime.Seconds; ViewLimitedTypes []*BeeperViewLimitedMedia
}
type CapabilitySupportLevel int // Partial(): >=1, Full(): >=2, Reject(): <=-2
const ( CapLevelRejected = -2; CapLevelDropped = -1; CapLevelUnsupported = 0; CapLevelPartialSupport = 1; CapLevelFullySupported = 2 )
const ( CapMsgVoice = "org.matrix.msc3245.voice"; CapMsgGIF = "fi.mau.gif"; CapMsgSticker = "m.sticker" ) // plus event.MsgImage/MsgVideo/MsgAudio/MsgFile as File keys
// Formatting keys: event.FmtBold, FmtItalic, FmtStrikethrough, FmtInlineCode, FmtCodeBlock, FmtBlockquote, FmtInlineLink, FmtUserLink, FmtRoomLink, FmtEventLink, FmtUnorderedList, FmtOrderedList, FmtSpoiler, FmtHeaders, FmtTable, FmtMath ...
```
What the bridge enforces (`bridgev2/portal.go`):
- **Replies/threads:** `msg.ReplyTo` is filled only if `caps.Reply.Partial() || caps.Thread.Partial()`. If `Reply` is not Partial, ReplyTo is set to nil. **Set `Reply: event.CapLevelFullySupported` or the connector never sees replies.**
- **Edits:** Matrix edits are dropped with `ErrEditsNotSupportedInPortal` unless `caps.Edit.Partial()`. `EditMaxAge` and `EditMaxCount` are also enforced (`ErrEditTargetTooOld` / `ErrEditTargetTooManyEdits`).
- **Files** (`checkMessageContentCaps`): msgtype missing from `File` → `ErrUnsupportedMessageType`; caption present and `Caption.Reject()` → `ErrCaptionsNotAllowed`; `MaxDuration` → `ErrMediaDurationTooLong`; `MaxSize` → `ErrMediaTooLarge`; mime rejected (`GetMimeSupport(...).Reject()`) → `ErrUnsupportedMediaType`.
- **Text:** no checks. `MaxTextLength` is advisory to clients ("message length is safer to check after conversion inside connector").
- **Reactions/deletes:** the bridge does not check the `Reaction`/`Delete` levels. Only the interface implementation matters. Still set them so clients show the UI.

Typical static return: a package-level `*event.RoomFeatures` with a stable `ID`, a `Formatting` map, `File` entries for `event.MsgImage`/`MsgVideo`/`MsgAudio`/`MsgFile` (each with `MimeTypes{"*/*": CapLevelFullySupported}`, `Caption`, `MaxSize`), `MaxTextLength`, `Reply`/`Edit`/`Delete`/`Reaction` = `CapLevelFullySupported`, and `ReadReceipts: true`. Bump the `capabilities` value of `GetBridgeInfoVersion` whenever this changes.

---

## 2. Login — `bridgev2/login.go`

```go
type LoginProcess interface {
	Start(ctx context.Context) (*LoginStep, error) // called once
	Cancel()                                       // cleanup; may be called after errors
}
type LoginProcessUserInput interface { LoginProcess
	SubmitUserInput(ctx context.Context, input map[string]string) (*LoginStep, error) }
type LoginProcessWithOverride interface { LoginProcess
	StartWithOverride(ctx context.Context, override *UserLogin) (*LoginStep, error) } // relogin
type LoginProcessWithParams interface { LoginProcess
	StartWithParams(ctx context.Context, params LoginStartParams) (*LoginStep, error) } // replaces Start+StartWithOverride
type LoginStartParams struct { Override *UserLogin; HTTP FingerprintingRoundTripper; WebSocketHTTP FingerprintingRoundTripper }
type LoginProcessDisplayAndWait interface { LoginProcess; Wait(ctx context.Context) (*LoginStep, error) }
type LoginProcessCookies interface { LoginProcess; SubmitCookies(ctx context.Context, cookies map[string]string) (*LoginStep, error) }
type LoginProcessStepCancel interface { LoginProcess; CancelStep(ctx context.Context) (*LoginStep, error) }

type LoginFlow struct { Name string `json:"name"`; Description string `json:"description"`; ID string `json:"id"` }

type LoginStepType string
const ( LoginStepTypeUserInput LoginStepType = "user_input"; LoginStepTypeCookies = "cookies"; LoginStepTypeClientHTTP = "client_http";
	LoginStepTypeDisplayAndWait = "display_and_wait"; LoginStepTypeWebAuthn = "webauthn"; LoginStepTypeComplete = "complete" ) // all LoginStepType

type LoginStep struct {
	Type         LoginStepType `json:"type"`
	StepID       string        `json:"step_id"`      // same per flow+step type, unique per bridge, e.g. "dev.example.traq.token"
	TxnID        string        `json:"txn_id"`       // random per step (retry-safety); provisioning fills it if empty [INFERENCE]
	Instructions string        `json:"instructions"` // human-readable
	// exactly one, matching Type:
	DisplayAndWaitParams *LoginDisplayAndWaitParams `json:"display_and_wait,omitempty"`
	CookiesParams        *LoginCookiesParams        `json:"cookies,omitempty"`
	ClientHTTPParams     *LoginClientHTTPParams     `json:"client_http,omitempty"`
	UserInputParams      *LoginUserInputParams      `json:"user_input,omitempty"`
	WebAuthnParams       *LoginWebAuthnParams       `json:"webauthn,omitempty"`
	CompleteParams       *LoginCompleteParams       `json:"complete,omitempty"`
}

type LoginUserInputParams struct {
	Fields      []LoginInputDataField       `json:"fields"`
	Attachments []*LoginUserInputAttachment `json:"attachments"` // sent as files by the bot before prompting
	CanCancel   bool                        `json:"can_cancel,omitzero"` // requires LoginProcessStepCancel
}

type LoginInputDataField struct {
	Type         LoginInputFieldType `json:"type"`
	ID           string              `json:"id"`          // key in submitted map
	Name         string              `json:"name"`        // shown to user
	Description  string              `json:"description"`
	DefaultValue string              `json:"default_value,omitempty"`
	Optional     bool                `json:"optional,omitzero"`
	Pattern      string              `json:"pattern,omitempty"` // regex; used by default Validate
	MinLength    int                 `json:"min_length,omitzero"`
	MaxLength    int                 `json:"max_length,omitzero"`
	Options      []string            `json:"options,omitempty"` // for select
	Validate     func(string) (string, error) `json:"-"`        // validate+clean; FillDefaultValidate() sets defaults
}
type LoginInputFieldType string
const ( LoginInputFieldTypeUsername = "username"; LoginInputFieldTypePassword = "password"; LoginInputFieldTypePhoneNumber = "phone_number";
	LoginInputFieldTypeEmail = "email"; LoginInputFieldType2FACode = "2fa_code"; LoginInputFieldTypeToken = "token"; LoginInputFieldTypeURL = "url";
	LoginInputFieldTypeDomain = "domain"; LoginInputFieldTypeSelect = "select"; LoginInputFieldTypeCaptchaCode = "captcha_code" ) // all LoginInputFieldType

type LoginCompleteParams struct { UserLoginID networkid.UserLoginID `json:"user_login_id"`; UserLogin *UserLogin `json:"-"` }
var ErrInvalidLoginFlowID error = RespError(mautrix.MNotFound.WithMessage("Invalid login flow ID")) // bridgev2/errors.go
```

### Completing a login — `bridgev2/userlogin.go`
```go
func (user *User) NewLogin(ctx context.Context, data *database.UserLogin, params *NewLoginParams) (*UserLogin, error)
type NewLoginParams struct {
	LoadUserLogin     func(context.Context, *UserLogin) error // default: Network.LoadUserLogin
	DeleteOnConflict  bool  // existing login with same ID owned by another Matrix user gets deleted (else error)
	DontReuseExisting bool  // error if same ID exists (default: reuse, update RemoteName/RemoteProfile/Metadata)
}
```
`NewLogin` sets `data.BridgeID` and `data.UserMXID`, and fills `data.Metadata` from `MetaTypes.UserLogin()` if nil. It disconnects any existing client with the same ID, then calls `LoadUserLogin`, which **must** set `ul.Client` or you get the error "client not filled by LoadUserLogin". Finally it inserts or updates the DB row.

`database.UserLogin` — `bridgev2/database/userlogin.go`:
```go
type UserLogin struct {
	BridgeID networkid.BridgeID; UserMXID id.UserID
	ID            networkid.UserLoginID   // e.g. traQ user UUID
	RemoteName    string                  // shown in list-logins / state notices
	RemoteProfile status.RemoteProfile    // {Phone, Email, Username, Name string; Avatar id.ContentURIString; AvatarFile *event.EncryptedFileInfo}
	SpaceRoom     id.RoomID
	Metadata      any                     // your *UserLoginMetadata (JSON column)
}
```
`bridgev2.UserLogin` embeds `*database.UserLogin` and adds `Bridge *Bridge; User *User; Log zerolog.Logger; Client NetworkAPI; BridgeState *BridgeStateQueue`. Persist metadata changes with `ul.Save(ctx) error`.

Return value from the final `SubmitUserInput`:
```go
&bridgev2.LoginStep{Type: bridgev2.LoginStepTypeComplete, StepID: "...complete", Instructions: "Successfully logged in as X",
	CompleteParams: &bridgev2.LoginCompleteParams{UserLoginID: ul.ID, UserLogin: ul}}
```
**The bridge does not call `Connect` after a new login.** `Client.Connect` is only called in `StartLoginsForUsers` at startup (`bridgev2/bridge.go`) and on reconnects (`bridgev2/bridgestate.go`). The connector must start the client itself after `NewLogin`. Use a background context, e.g. `ul.Bridge.BackgroundCtx` with the login's logger, not the request ctx.

### How bot commands render it — `bridgev2/commands/login.go`
- `login [flow ID]`: one flow is auto-picked; with several, it replies "Please specify a login flow, e.g. `login <id>`." plus ``* `id` - Description`` lines. Then it calls `CreateLogin` and `StartWithParams` / `StartWithOverride` / `Start`, in that order of preference.
- `doLoginStep`: if `step.Instructions != ""`, the bot first replies with the Instructions text (markdown).
- `user_input`: attachments are sent first. Then **one field at a time** the bot replies:
  ```
  Please enter your <Field.Name>
  <Field.Description>          (if set)
  Options: `a`, `b`            (if Options)
  Enter `skip` to skip this field   (if Optional)
  ```
  The user's **next message** in the room is taken whole (`ce.RawArgs`) as the value; it goes through `FillDefaultValidate()` and then `Validate`. Fields of type `password`/`token` make the bot **redact** the user's message. After the last field it calls `SubmitUserInput(ctx, map[id]value)`, then `doLoginStep` again on the result.
- Prefix: in the management room any message counts, so the raw value works. In other rooms the value must be prefixed with the command prefix (the bot warns "This is not your management room...").
- Inline shortcut: `login <flow> <v1> <v2>...` supplies all fields at once. The number of values must equal the number of fields.
- `complete`: nothing is printed besides `Instructions`, so put the success text there. On relogin with a different ID, the old login is deleted (`LOGIN_OVERRIDDEN`).
- `cancel` calls `LoginProcess.Cancel`. `client_http` steps are unsupported in chat. While input is pending, a message whose first word is a registered command name runs that command instead.

---

## 3. Remote events

### Queueing — `bridgev2/queue.go`
```go
func (ul *UserLogin) QueueRemoteEvent(evt RemoteEvent) EventHandlingResult
func (br *Bridge) QueueRemoteEvent(login *UserLogin, evt RemoteEvent) EventHandlingResult
type EventHandlingResult struct { Success, Ignored, Queued, SkipStateEcho bool; Error error; SendMSS bool; EventID id.EventID; StreamOrder int64 }
```
`QueueRemoteEvent` does `GetPortalByKey`, which creates the portal **DB row** if missing, then queues the event into that portal's serial event loop.

When the portal has no Matrix room, the event is **dropped** unless it implements `RemoteEventThatMayCreatePortal` with `ShouldCreatePortal()` true (`EventMeta.CreatePortal`) and `bridge.portal_create_filter` allows it.

Room creation then uses `GetChatInfo` from the event if it is a `RemoteChatResyncWithInfo`. Otherwise, or if `info.Members == nil`, it calls `source.Client.GetChatInfo(ctx, portal)` (`createMatrixRoomInLoop`, `bridgev2/portal.go`). A `ChatResync` that creates the room is not processed further.

### `RemoteEventType` — `bridgev2/networkinterface.go`
```go
const ( RemoteEventUnknown RemoteEventType = iota; RemoteEventMessage; RemoteEventMessageUpsert; RemoteEventEdit;
	RemoteEventReaction; RemoteEventReactionRemove; RemoteEventReactionSync; RemoteEventMessageRemove;
	RemoteEventReadReceipt; RemoteEventDeliveryReceipt; RemoteEventMarkUnread; RemoteEventTyping;
	RemoteEventChatInfoChange; RemoteEventChatResync; RemoteEventChatDelete; RemoteEventBackfill )
```
Dispatch is by `GetType()`: the switch in `handleRemoteEvent` type-asserts the event to the matching interface. **You must set `EventMeta.Type` yourself.** A wrong type means it is ignored or the type assertion panics (the panic is recovered by the portal loop).

### `simplevent` — `bridgev2/simplevent/*.go`
```go
// meta.go
type EventMeta struct {
	Type              bridgev2.RemoteEventType
	LogContext        func(c zerolog.Context) zerolog.Context
	PortalKey         networkid.PortalKey
	UncertainReceiver bool                 // look up existing portal with/without receiver
	Sender            bridgev2.EventSender
	CreatePortal      bool                 // allow creating the Matrix room for this event
	Timestamp         time.Time            // zero → time.Now()
	StreamOrder       int64
	PreHandleFunc           func(context.Context, *bridgev2.Portal)
	PreHandleWithResultFunc func(context.Context, *bridgev2.Portal) *bridgev2.EventHandlingResult // non-nil result short-circuits
	PostHandleFunc          func(context.Context, *bridgev2.Portal)
	MutateContextFunc       func(context.Context) context.Context
	FetchCertainPortalKeyFunc func(context.Context) networkid.PortalKey
}
// value-receiver builders: WithType, WithLogContext, WithMoreLogContext, WithPortalKey, WithUncertainReceiver,
// WithSender, WithCreatePortal, WithTimestamp, WithStreamOrder  (each returns EventMeta)

// message.go
type Message[T any] struct {      // RemoteMessage + RemoteEdit + RemoteMessageUpsert + RemoteMessageWithTransactionID
	EventMeta
	Data T
	ID            networkid.MessageID
	TransactionID networkid.TransactionID
	TargetMessage networkid.MessageID   // for RemoteEventEdit
	ConvertMessageFunc func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, data T) (*bridgev2.ConvertedMessage, error)
	ConvertEditFunc    func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, existing []*database.Message, data T) (*bridgev2.ConvertedEdit, error)
	HandleExistingFunc func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, existing []*database.Message, data T) (bridgev2.UpsertResult, error)
}
type PreConvertedMessage struct { EventMeta; Data *bridgev2.ConvertedMessage; ID networkid.MessageID; TransactionID networkid.TransactionID
	HandleExistingFunc func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, existing []*database.Message) (bridgev2.UpsertResult, error) }
type MessageRemove struct { EventMeta; TargetMessage networkid.MessageID; OnlyForMe bool; HidePlaceholder bool }

// reaction.go
type Reaction struct {             // RemoteReaction (+WithMeta, +WithExtraContent) AND RemoteReactionRemove
	EventMeta
	TargetMessage  networkid.MessageID
	EmojiID        networkid.EmojiID  // dedup key; for removal lookup by (target, sender, EmojiID)
	Emoji          string
	ExtraContent   map[string]any
	ReactionDBMeta any
}
type ReactionSync struct {
	EventMeta
	TargetMessage networkid.MessageID
	Reactions     *bridgev2.ReactionSyncData // {Users map[networkid.UserID]*ReactionSyncUser{Reactions []*BackfillReaction; HasAllReactions bool; MaxCount int}; HasAllUsers bool}
}

// receipt.go
type Receipt struct {              // RemoteReadReceipt (+WithThread) and RemoteDeliveryReceipt — pick via Type
	EventMeta
	LastTarget networkid.MessageID; Targets []networkid.MessageID; ReadUpTo time.Time; ThreadRoot networkid.MessageID; ReadUpToStreamOrder int64
}
type MarkUnread struct { EventMeta; Unread bool }
type Typing struct { EventMeta; Timeout time.Duration; Type bridgev2.TypingType } // TypingTypeText/UploadingMedia/RecordingMedia

// chat.go
type ChatResync struct {
	EventMeta
	ChatInfo        *bridgev2.ChatInfo
	GetChatInfoFunc func(ctx context.Context, portal *bridgev2.Portal) (*bridgev2.ChatInfo, error)
	LatestMessageTS        time.Time
	CheckNeedsBackfillFunc func(ctx context.Context, latestMessage *database.Message) (bool, error)
	BundledBackfillData    any
}
type ChatDelete struct { EventMeta; OnlyForMe bool; Children bool }
type ChatInfoChange struct { EventMeta; ChatInfoChange *bridgev2.ChatInfoChange }
type Backfill struct { EventMeta; Data *bridgev2.FetchMessagesResponse
	GetDataFunc func(ctx context.Context, portal *bridgev2.Portal) (*bridgev2.FetchMessagesResponse, error) }
```
Helper interfaces worth knowing: `RemoteEventWithTargetPart{GetTargetMessagePart() networkid.PartID}`, `RemoteMessageRemoveWithoutPlaceholder`, `UpsertResult{SubEvents []RemoteEvent; SaveParts bool; ContinueMessageHandling bool}`.

### `EventSender` — `bridgev2/networkinterface.go`
```go
type EventSender struct {
	IsFromMe    bool                  // sender = the receiving UserLogin; uses double puppet if available
	SenderLogin networkid.UserLoginID // login that sent it (may differ from receiving login)
	Sender      networkid.UserID      // remote user ID → ghost
	ForceDMUser, ForceEditOrigSender bool
}
```

Dedup of incoming messages (`handleRemoteMessage`): first the pending outgoing transaction ID check (`checkPendingMessage`), then `DB.Message.GetAllPartsByID(ctx, portal.Receiver, id)`; existing parts → ignored as duplicate unless the type is `MessageUpsert`.

Returning `bridgev2.ErrIgnoringRemoteEvent` from a Convert func silently drops the event. Any other error sends an "An error occurred while processing an incoming message" notice into the room.

---

## 4. Chat / user info

### `bridgev2/portal.go`
```go
type ChatInfo struct {
	Name   *string             // bridgev2.DefaultChatName resets to member-based default
	Topic  *string
	Avatar *Avatar
	Members  *ChatMemberList   // nil → on create, bridge refetches GetChatInfo
	JoinRule *event.JoinRulesEventContent
	Type      *database.RoomType
	Disappear *database.DisappearingSetting
	ParentID  *networkid.PortalID   // parent space portal
	UserLocal      *UserLocalPortalInfo
	MessageRequest *bool
	UserBlocked    *bool
	CanBackfill    bool            // REQUIRED true for initial forward backfill & queue task
	ExcludeChangesFromTimeline bool
	ExtraUpdates ExtraUpdater[*Portal] // func(context.Context, *Portal) bool (changed)
}
type ChatMemberList struct {
	IsFull                     bool  // true: members not listed are removed
	CheckAllLogins             bool  // call IsThisUser on every login (discouraged)
	ExcludeChangesFromTimeline bool
	TotalMemberCount           int
	OtherUserID                networkid.UserID // DM recipient; auto-filled if MemberMap has 2 entries
	Members     []ChatMember     // Deprecated
	MemberMap   ChatMemberMap    // map[networkid.UserID]ChatMember; .Set(m) / .Add(m) bool
	PowerLevels *PowerLevelOverrides
}
type ChatMember struct {
	EventSender                       // IsFromMe/SenderLogin/Sender
	Membership event.Membership       // event.MembershipJoin etc.
	Nickname *string; PowerLevel *int
	UserInfo *UserInfo                // sync ghost while syncing members
	MemberSender EventSender; MemberEventExtra map[string]any; PrevMembership event.Membership
}
type ChatInfoChange struct {
	ChatInfo      *ChatInfo        // only changed fields
	MemberChanges *ChatMemberList  // only changes; full resync via ChatInfo.Members
}
type UserLocalPortalInfo struct {
	MutedUntil *time.Time   // event.MutedForever (event/accountdata.go) = forever; bridgev2.Unmuted (2024-01-01) = unmute
	Tag        *event.RoomTag
}
type PowerLevelOverrides struct { Events map[event.Type]int; UsersDefault, EventsDefault, StateDefault, Invite, Kick, Ban, Redact *int; Custom func(*event.PowerLevelsEventContent) bool }
```

### `bridgev2/ghost.go`
```go
type Avatar struct {
	ID     networkid.AvatarID                    // change detection: same ID ⇒ no refetch
	Get    func(ctx context.Context) ([]byte, error) // lazy fetch; bridge sha256s, detects mime, uploads via intent.UploadMedia
	Remove bool
	MXC    id.ContentURIString                   // pre-uploaded alternative
	Hash   [32]byte
}
type UserInfo struct { Identifiers []string; Name *string; Avatar *Avatar; IsBot *bool; ExtraProfile database.ExtraProfile; ExtraUpdates ExtraUpdater[*Ghost] }
```
`GetUserInfo` is called through `ghost.UpdateInfoIfNecessary` when the ghost lacks a name or avatar flag, or when `AggressiveUpdateInfo` is set. Ghost fields (`database.Ghost`): `ID, Name, AvatarID, AvatarHash, AvatarMXC, NameSet, AvatarSet, ContactInfoSet, IsBot, Identifiers, ExtraProfile, Metadata`.

### `RoomType` — `bridgev2/database/portal.go`
```go
const ( RoomTypeDefault RoomType = ""; RoomTypeDM = "dm"; RoomTypeGroupDM = "group_dm"; RoomTypeSpace = "space" )
```
`database.Portal` fields: `BridgeID`, embedded `networkid.PortalKey`, `MXID id.RoomID`, `ParentKey`, `RelayLoginID`, `OtherUserID`, `Name`, `Topic`, `AvatarID`, `AvatarHash`, `AvatarMXC`, `NameSet`, `TopicSet`, `AvatarSet`, `NameIsCustom`, `InSpace`, `MessageRequest`, `UserBlocked`, `RoomType`, `Disappear`, `CapState`, `Metadata`. `bridgev2.Portal` embeds `*database.Portal` and adds `Bridge`, `Log`, etc.

### `networkid` — `bridgev2/networkid/bridgeid.go`
```go
type BridgeID string; type PortalID string; type UserID string; type UserLoginID string
type MessageID string; type PartID string; type EmojiID string; type AvatarID string
type TransactionID string; type RawTransactionID string; type PaginationCursor string; type MediaID []byte
type PortalKey struct {
	ID       PortalID    `json:"portal_id"`
	Receiver UserLoginID `json:"portal_receiver,omitempty"`
}   // IsEmpty(), String() = "id" or "id/receiver"
type MessagePartID struct { MessageID MessageID; PartID PartID }
type MessageOptionalPartID struct { MessageID MessageID; PartID *PartID }
```
`PortalKey.Receiver`: leave empty for group chats/channels (all logins share one room); set to the login ID for DMs (each logged-in user gets their own DM room even when both sides are logged in); mandatory everywhere with `bridge.split_portals: true`. Message lookups take `portal.Receiver` and match `room_receiver=$x OR room_receiver=''`. `UserID`s must be globally unique within the bridge.

---

## 5. Message conversion, Matrix side

### Converted types — `bridgev2/networkinterface.go`
```go
type ConvertedMessagePart struct {
	ID         networkid.PartID           // "" for single-part
	Type       event.Type                 // event.EventMessage (or EventSticker)
	Content    *event.MessageEventContent
	Extra      map[string]any
	DBMetadata any                        // stored in database.Message.Metadata
	DontBridge bool                       // stored with fake "~fake:" MXID, not sent
}
type ConvertedMessage struct {
	ReplyTo *networkid.MessageOptionalPartID
	ReplyToRoom networkid.PortalKey; ReplyToUser networkid.UserID; ReplyToLogin networkid.UserLoginID // Beeper backfill only
	ThreadRoot *networkid.MessageID
	Parts      []*ConvertedMessagePart
	Disappear  database.DisappearingSetting
}
func (cm *ConvertedMessage) MergeCaption() bool
func MergeCaption(textPart, mediaPart *ConvertedMessagePart) *ConvertedMessagePart
type ConvertedEditPart struct {
	Part          *database.Message           // existing DB part being edited
	Type          event.Type
	Content       *event.MessageEventContent  // put inside m.new_content automatically; DON'T call SetEdit
	Extra         map[string]any
	TopLevelExtra map[string]any
	NewMentions   *event.Mentions
	DontBridge    bool
}
func (cmp *ConvertedMessagePart) ToEditPart(part *database.Message) *ConvertedEditPart
type ConvertedEdit struct {
	ModifiedParts []*ConvertedEditPart
	DeletedParts  []*database.Message
	AddedParts    *ConvertedMessage  // appended at end of room
}
```

### Matrix event types — `bridgev2/networkinterface.go`
```go
type MatrixEventBase[ContentType any] struct {
	Event      *event.Event
	Content    ContentType
	Portal     *Portal
	OrigSender *OrigSender          // non-nil when relayed
	InputTransactionID networkid.RawTransactionID
}
type MatrixMessage struct { MatrixEventBase[*event.MessageEventContent]; ThreadRoot *database.Message; ReplyTo *database.Message }
type MatrixEdit struct { MatrixEventBase[*event.MessageEventContent]; EditTarget *database.Message } // Content is already m.new_content
type MatrixReaction struct {
	MatrixEventBase[*event.ReactionEventContent]   // emoji: msg.Content.RelatesTo.Key
	TargetMessage           *database.Message
	PreHandleResp           *MatrixReactionPreResponse
	ReactionToOverride      *database.Reaction
	ExistingReactionsToKeep []*database.Reaction
}
type MatrixReactionRemove struct { MatrixEventBase[*event.RedactionEventContent]; TargetReaction *database.Reaction }
type MatrixMessageRemove  struct { MatrixEventBase[*event.RedactionEventContent]; TargetMessage *database.Message }
type MatrixMessageResponse struct {
	DB            *database.Message   // set ID + SenderID (+PartID, Metadata); nil → error logged, nothing saved
	StreamOrder   int64
	Disappear     *database.DisappearingSetting
	Pending       bool                 // only with msg.AddPendingToSave
	RemovePending networkid.TransactionID // with msg.AddPendingToIgnore
	PostSave      func(context.Context, *database.Message)
}
func (evt *MatrixMessage) AddPendingToIgnore(txnID networkid.TransactionID)
func (evt *MatrixMessage) AddPendingToSave(message *database.Message, txnID networkid.TransactionID, handleEcho RemoteEchoHandler)
func (evt *MatrixMessage) RemovePending(txnID networkid.TransactionID)
```
Fields the bridge fills on `resp.DB` if empty (`fillDBMessage`, `bridgev2/portal.go`): `MXID` (event ID), `Room` (portal key), `Timestamp` (event ts), `ReplyTo`, `ThreadRoot`, `SenderMXID`, `SendTxnID`. It does **not** fill `ID` or `SenderID`.

Before saving, it also calls `GetGhostByID(ctx, message.SenderID)` so the ghost row exists.

### `MatrixAPI` (intents; `br.Bot`, `ghost.Intent`, double puppet) — `bridgev2/matrixinterface.go`
```go
UploadMedia(ctx context.Context, roomID id.RoomID, data []byte, fileName, mimeType string) (url id.ContentURIString, file *event.EncryptedFileInfo, err error)
UploadMediaStream(ctx context.Context, roomID id.RoomID, size int64, requireFile bool, cb FileStreamCallback) (url id.ContentURIString, file *event.EncryptedFileInfo, err error)
DownloadMedia(ctx context.Context, uri id.ContentURIString, file *event.EncryptedFileInfo) ([]byte, error)
DownloadMediaToFile(ctx context.Context, uri id.ContentURIString, file *event.EncryptedFileInfo, writable bool, callback func(*os.File) error) error
SendMessage(ctx context.Context, roomID id.RoomID, eventType event.Type, content *event.Content, extra *MatrixSendExtra) (*mautrix.RespSendEvent, error)
MarkRead(ctx context.Context, roomID id.RoomID, eventID id.EventID, ts time.Time) error
MuteRoom(ctx context.Context, roomID id.RoomID, until time.Time) error
GetMXID() id.UserID; IsDoublePuppet() bool
```
Media handling:
- **Outgoing (Matrix → traQ):** call `portal.Bridge.Bot.DownloadMedia(ctx, msg.Content.URL, msg.Content.File)`. `File` is non-nil for encrypted rooms; in that case use `File.URL`.
- **Incoming (traQ → Matrix):** call `intent.UploadMedia(ctx, portal.MXID, data, name, mime)` inside `ConvertMessageFunc`. Put the result in `Content.URL` (unencrypted) or `Content.File` (encrypted room).

Also on `MatrixConnector` (`br.Matrix`): `ParseGhostMXID(id.UserID) (networkid.UserID, bool)`, `FormatGhostMXID(networkid.UserID) id.UserID`, `GhostIntent(networkid.UserID) MatrixAPI`, `ServerName() string`.

### `database.Message` — `bridgev2/database/message.go`
```go
type Message struct {
	RowID int64; BridgeID networkid.BridgeID; ID networkid.MessageID; PartID networkid.PartID; MXID id.EventID
	Room networkid.PortalKey; SenderID networkid.UserID; SenderMXID id.UserID; Timestamp time.Time; EditCount int; IsDoublePuppeted bool
	ThreadRoot networkid.MessageID; ReplyTo networkid.MessageOptionalPartID; SendTxnID networkid.RawTransactionID; Metadata any
}   // HasFakeMXID() for DontBridge parts
type Reaction struct { BridgeID; Room networkid.PortalKey; MessageID networkid.MessageID; MessagePartID networkid.PartID;
	SenderID networkid.UserID; SenderMXID id.UserID; EmojiID networkid.EmojiID; MXID id.EventID; Timestamp time.Time; Emoji string; Metadata any } // database/reaction.go
```
Lookups (`br.DB.Message.*`, receiver = `portal.Receiver`):
```go
GetPartByID(ctx, receiver networkid.UserLoginID, id networkid.MessageID, partID networkid.PartID) (*Message, error)
GetFirstPartByID(ctx, receiver networkid.UserLoginID, id networkid.MessageID) (*Message, error)     // ORDER BY part_id ASC
GetLastPartByID(ctx, receiver networkid.UserLoginID, id networkid.MessageID) (*Message, error)
GetAllPartsByID(ctx, receiver networkid.UserLoginID, id networkid.MessageID) ([]*Message, error)
GetFirstOrSpecificPartByID(ctx, receiver networkid.UserLoginID, id networkid.MessageOptionalPartID) (*Message, error)
GetPartByMXID(ctx, mxid id.EventID) (*Message, error)                       // by Matrix event ID
GetLastPartAtOrBeforeTime(ctx, portal networkid.PortalKey, maxTS time.Time) (*Message, error)
GetFirstPortalMessage(ctx, portal networkid.PortalKey) (*Message, error)
GetLastNInPortal(ctx, portal networkid.PortalKey, n int) ([]*Message, error)
```
All return `(nil, nil)` when not found [INFERENCE: dbutil QueryOne semantics; bridge code checks `== nil` after `err == nil`]. Reactions: `br.DB.Reaction.GetByID(ctx, receiver, messageID, partID, senderID, emojiID)`, `GetByMXID(ctx, mxid)`, `GetAllToMessage(ctx, receiver, messageID)`.

Portals / ghosts — `bridgev2/portal.go`, `bridgev2/ghost.go`:
```go
func (br *Bridge) GetPortalByKey(ctx context.Context, key networkid.PortalKey) (*Portal, error)          // creates DB row if missing (no Matrix room)
func (br *Bridge) GetExistingPortalByKey(ctx context.Context, key networkid.PortalKey) (*Portal, error)  // nil if none; with Receiver set & !SplitPortals also tries receiver-less key
func (br *Bridge) GetPortalByMXID(ctx context.Context, mxid id.RoomID) (*Portal, error)
func (br *Bridge) GetGhostByID(ctx context.Context, id networkid.UserID) (*Ghost, error)                 // creates
func (br *Bridge) GetExistingGhostByID(ctx context.Context, id networkid.UserID) (*Ghost, error)
func (br *Bridge) GetGhostByMXID(ctx context.Context, mxid id.UserID) (*Ghost, error)
func (br *Bridge) GetExistingUserByMXID(ctx context.Context, userID id.UserID) (*User, error)
func (br *Bridge) GetExistingUserLoginByID(ctx context.Context, id networkid.UserLoginID) (*UserLogin, error)
func (br *Bridge) GetCachedUserLoginByID(id networkid.UserLoginID) *UserLogin
func (br *Bridge) GetUserLoginsInPortal(ctx context.Context, portal networkid.PortalKey) ([]*UserLogin, error)
```
Permalink for an existing message (`id/opaque.go`, `id/matrixuri.go`):
```go
func (roomID RoomID) EventURI(eventID EventID, via ...string) *MatrixURI
func (uri *MatrixURI) MatrixToURL() string   // "https://matrix.to/#/%21room:server/$event"
// usage: portal.MXID.EventURI(dbMsg.MXID, br.Matrix.ServerName()).MatrixToURL()
// also id.UserID.URI(), id.RoomID.URI(via...)
```

---

## 6. Metadata & own DB tables

`bridgev2/database/database.go`:
```go
type MetaTypeCreator func() any
type MetaTypes struct { Portal, Ghost, Message, Reaction, UserLogin MetaTypeCreator }
type MetaMerger interface { CopyFrom(other any) } // optional on your meta types (used by NewLogin/ToEditPart merges)
type Database struct {
	*dbutil.Database          // embedded: the raw go.mau.fi/util dbutil DB is exposed as br.DB.Database
	BridgeID networkid.BridgeID
	Portal *PortalQuery; Ghost *GhostQuery; Message *MessageQuery; DisappearingMessage *DisappearingMessageQuery
	Reaction *ReactionQuery; User *UserQuery; UserLogin *UserLoginQuery; UserPortal *UserPortalQuery
	BackfillTask *BackfillTaskQuery; KV *KVQuery; PublicMedia *PublicMediaQuery; EncryptedDirectMedia *EncryptedDirectMediaQuery
}
```
Metadata pattern: `GetDBMetaTypes` returns creators, e.g. `UserLogin: func() any { return &UserLoginMetadata{} }` (nil creator = blank meta). The value is JSON-serialized into the `metadata` column. Read with `login.Metadata.(*UserLoginMetadata)`; persist with `login.Save(ctx)` (portals: `portal.Save(ctx)`).

Bridge's own upgrades:
- `bridgev2/database/upgrades/upgrades.go`:
  ```go
  //go:embed *.sql
  var rawUpgrades embed.FS
  var Table = dbutil.BuildUpgradeTable().WithFS(rawUpgrades).With(upgradeV30).Finish()
  ```
- `database.New` sets `db.UpgradeTable = upgrades.Table`; `Bridge.StartConnectors` runs `br.DB.Upgrade(ctx)` before `Network.Start` (skipped if `ExternallyManagedDB`).
- SQL headers: `00-latest.sql` starts `-- v0 -> v33 (compatible with v9+): Latest revision`; incremental files `-- v2 (compatible with v1+): Add ...`, optionally followed by `-- only: postgres` / `-- only: sqlite` and `-- transaction: off`.

Connector-owned table, same pattern as `sqlstatestore/statestore.go` and `crypto/sql_store.go`:
```go
const VersionTableName = "traq_version"
//go:embed *.sql
var upgradesFS embed.FS
var UpgradeTable = dbutil.BuildUpgradeTable().WithFS(upgradesFS).Finish()

// in Start(ctx):  (br.DB.Database is *dbutil.Database)
child := tc.br.DB.Database.Child(VersionTableName, UpgradeTable, dbutil.ZeroLogger(tc.br.Log.With().Str("db_section", "traq").Logger()))
if err := child.Upgrade(ctx); err != nil { return bridgev2.DBUpgradeError{Section: "traq", Err: err} }
```
Signature of `Child` is inferred from call sites (`db.Child(name, table, log)`): the go.mau.fi/util v0.11.1 source is not in the local module cache. mxmain treats `bridgev2.DBUpgradeError` returned from Start specially (`LogDBUpgradeErrorAndExit`). There is also a simple KV store, `br.DB.KV.Get(ctx, database.Key) string` / `Set(ctx, key, value)` (`database/kvstore.go`). It is bridge-global and keyed by `database.Key` strings.

---

## 7. Formatting — `format/htmlparser.go`, `format/markdown.go`
```go
type Context struct { Ctx context.Context; ReturnData map[string]any; TagStack TagStack; PreserveWhitespace bool }
func NewContext(ctx context.Context) Context
func (ctx Context) Mentions() *event.Mentions
type PillConverter func(displayname, mxid, eventID string, ctx Context) string
type LinkConverter func(text, href string, ctx Context) string
type TextConverter func(string, Context) string
type SpoilerConverter func(text, reason string, ctx Context) string
type CodeBlockConverter func(code, language string, ctx Context) string
type ImageConverter func(src, alt, title, width, height string, isEmoji bool) string
type HTMLParser struct {
	PillConverter PillConverter; TabsToSpaces int; Newline string; HorizontalLine string
	BoldConverter, ItalicConverter, StrikethroughConverter, UnderlineConverter, MathConverter, MathBlockConverter TextConverter
	LinkConverter LinkConverter; SpoilerConverter SpoilerConverter; ColorConverter ColorConverter
	MonospaceBlockConverter CodeBlockConverter; MonospaceConverter TextConverter; TextConverter TextConverter; ImageConverter ImageConverter
}
func (parser *HTMLParser) Parse(htmlData string, ctx Context) string
func DefaultPillConverter(displayname, mxid, eventID string, ctx Context) string // users → displayname (+adds mention)
func DefaultMonospaceBlockConverter(code, language string, ctx Context) string
var TextHTMLParser, MarkdownHTMLParser *HTMLParser
func HTMLToText(html string) string
func HTMLToMarkdown(html string) string
func HTMLToMarkdownFull(parser *HTMLParser, html string) (parsed string, mentions *event.Mentions) // nil parser = MarkdownHTMLParser
func RenderMarkdown(text string, allowMarkdown, allowHTML bool) event.MessageEventContent  // VALUE, not pointer; goldmark + strikethrough, table, spoiler; hard wraps
func HTMLToContent(html string) event.MessageEventContent
func TextToContent(text string) event.MessageEventContent
func EscapeMarkdown(text string) string
func MarkdownMention(id uriAble) string; func MarkdownMentionWithName(name string, id uriAble) string; func MarkdownLink(name, url string) string
func SafeMarkdownCode[T ~string](textInput T) string
```
Defaults with nil converters: bold/italic/strike/code → `**x**`, `_x_`, `~~x~~`, backtick code; underline → plain text (`TextHTMLParser`) or `<u>` (`MarkdownHTMLParser`).
- **Links:** `<a href>` is tried as a matrix.to / matrix: URI first. On success, `PillConverter(text, PrimaryIdentifier, SecondaryIdentifier, ctx)` is called (for event links `mxid` = room ID and `eventID` = event ID). Otherwise `LinkConverter` runs, else the output is `text (href)`.

Matrix HTML → traQ markdown: copy `*format.MarkdownHTMLParser` (struct copy) and set a custom `PillConverter`. In the callback, map ghosts with `br.Matrix.ParseGhostMXID(id.UserID(mxid))` (e.g. emit `@username`); map real Matrix users via `br.GetExistingUserByMXID` and their login. Then call `parser.Parse(content.FormattedBody, format.NewContext(ctx))` when `content.Format == event.FormatHTML`, else use `content.Body`.

traQ → Matrix: `format.RenderMarkdown(md, true, false)`, then `content.Mentions = &event.Mentions{UserIDs: ...}`. Return `&content` in a part with `Type: event.EventMessage`.

---

## 8. Bot commands — `bridgev2/commands/*.go`
```go
type FullHandler struct {                      // handler.go
	Func    func(*Event)
	Name    string
	Aliases []string
	Help    HelpMeta                           // {Command string; Section HelpSection; Description string; Args string}
	RequiresAdmin           bool
	RequiresPortal          bool               // must be run in a portal room (ce.Portal != nil)
	RequiresLogin           bool               // ce.User.GetDefaultLogin() != nil
	RequiresEventLevel      event.Type
	RequiresLoginPermission bool
	NetworkAPI       ImplementationChecker[bridgev2.NetworkAPI]       // e.g. commands.NetworkAPIImplements[bridgev2.UserSearchingNetworkAPI]
	NetworkConnector ImplementationChecker[bridgev2.NetworkConnector] // hides/blocks command if not implemented
}
// help.go sections: HelpSectionGeneral{0}, HelpSectionAuth{10}, HelpSectionChats{20}, HelpSectionMisc{30}, HelpSectionAdmin{50}
type Event struct {                             // event.go
	Bot bridgev2.MatrixAPI; Bridge *bridgev2.Bridge; Portal *bridgev2.Portal; Processor *Processor
	Handler MinimalCommandHandler; RoomID id.RoomID; OrigRoomID id.RoomID; EventID id.EventID
	User *bridgev2.User; Command string; Args []string; RawArgs string; ReplyTo id.EventID
	Ctx context.Context; Log *zerolog.Logger; Sudo bool; MessageStatus *bridgev2.MessageStatus
}
func (ce *Event) Reply(msg string, args ...any) id.EventID   // replaces $cmdprefix; Sprintf only if args; markdown; m.notice
func (ce *Event) ReplyAdvanced(msg string, allowMarkdown, allowHTML bool) id.EventID
func (ce *Event) React(key string) id.EventID
func (ce *Event) Redact(req ...mautrix.ReqRedact)
func (ce *Event) MarkRead()
func (proc *Processor) AddHandler(handler CommandHandler)     // processor.go; map by name → OVERWRITES same name
func (proc *Processor) AddHandlers(handlers ...CommandHandler)
func NewProcessor(bridge *bridgev2.Bridge) bridgev2.CommandProcessor
```
Registration: `Bridge.Commands` is typed `bridgev2.CommandProcessor`, so a type assertion is needed:
```go
func (tc *TraqConnector) Init(br *bridgev2.Bridge) {
	tc.br = br
	br.Commands.(*commands.Processor).AddHandlers(cmdSearchMessages)
}
```
Ordering in `bridgev2.NewBridge` (`bridgev2/bridge.go`): `br.Commands = newCommandProcessor(br)` → `Matrix.Init` (adds `login-matrix`, `ping-matrix`, `logout-matrix`, `set-pl`, `discard-megolm-session`; `bridgev2/matrix/connector.go:175`) → `Network.Init`.

So registering in **`Init`** works, and so does `Start`. In-repo examples: `bridgev2/matrix/connector.go:175` (`AddHandlers`) and `bridgev2/matrix/mxmain/main.go:260` (`AddHandler` for `version`, done after `NewBridge`, so it overrides any connector `version`).

**Name clash:** there is a built-in **`search`** command (`commands/startchat.go`, `CommandSearch`) that searches users via `UserSearchingNetworkAPI`. Registering your own `search` replaces it, because handlers are stored by name and the last registration wins. Pick e.g. `search-messages` unless you want that override.

Routing (`bridgev2/queue.go`): a message is a command if its body starts with `bridge.command_prefix` or it is in the user's management room; prefix + one space is trimmed. Commands run in a goroutine (`go br.Commands.Handle(...)`). The command word is lowercased; `RawArgs` is everything after it, leading spaces trimmed.

---

## 9. `mxmain.BridgeMain` — `bridgev2/matrix/mxmain/main.go`
```go
type BridgeMain struct {
	Name        string   // project name (don't use mautrix-*)
	Description string
	URL         string
	Version     string   // compared to git tag in InitVersion
	SemCalVer   bool
	PostInit    func()
	PostStart   func()
	DBOwner     string   // default "megabridge/"+Name
	PostMigratePortal func(context.Context, *bridgev2.Portal) error
	Connector   bridgev2.NetworkConnector
	// set automatically:
	Log *zerolog.Logger; DB *dbutil.Database; Config *bridgeconfig.Config; Matrix *matrix.Connector; Bridge *bridgev2.Bridge
	ConfigPath, RegistrationPath string; SaveConfig bool
	AdditionalShortFlags, AdditionalLongFlags string
}
func (br *BridgeMain) InitVersion(tag, commit, rawBuildTime string)
func (br *BridgeMain) Run() // PreInit → Init → Start → WaitForInterrupt → Stop → os.Exit
```
Minimal `main.go`:
```go
var (Tag = "unknown"; Commit = "unknown"; BuildTime = "unknown") // -ldflags "-X main.Tag=... -X main.Commit=... -X 'main.BuildTime=...'"
func main() {
	m := mxmain.BridgeMain{Name: "matrix-traq", URL: "https://...", Description: "A Matrix-traQ puppeting bridge.", Version: "0.1.0", Connector: &connector.TraqConnector{}}
	m.InitVersion(Tag, Commit, BuildTime)
	m.Run()
}
```
CLI flags:
- `-c/--config` (default `config.yaml`)
- `-e/--generate-example-config`: writes the full example (network section + base) to the config path, or stdout if `-`. It refuses if the file exists, then exits. It calls `Connector.GetConfig()` and `Connector.GetName()` **before** config load or `Init`.
- `-g/--generate-registration`: loads and upgrades the config, writes `registration.yaml` (`-r` path), writes `as_token`/`hs_token` back into the config, then exits. It requires `homeserver.domain` to be set and is incompatible with `-n`.
- `-n` (don't save config), `-v`, `--version-json`, `--ignore-unsupported-database`, `--ignore-foreign-tables`, `--ignore-unsupported-server`.

`Init()` aborts on unconfigured `homeserver.address`/`domain`, `as_token`/`hs_token`, `database.uri`, or `bridge.permissions`, or on a `ConfigValidatingNetwork.ValidateConfig()` error.

Network config embedding (`mxmain/config.go`, `main.go getConfigUpgrader`):
- **Example text:** `makeFullExampleConfig` writes `network:` and then your `example` string with every line indented by 4 spaces. Write the example at top level, **unindented**, without a `network:` key. The base config template uses `$<< >>` delimiters with `BridgeName` (e.g. `$<<.NetworkID>>`).
- **Upgrader:** yours is wrapped in `configupgrade.ProxyUpgrader{Target: upgrader, Prefix: []string{"network"}}`, so `helper.Copy` paths are relative to `network`. A nil upgrader becomes `NoopUpgrader`.
- **Decoding:** after upgrade, `cfg.Network.Decode(networkData)` fills your `data`, which must be a pointer with yaml tags.
```go
//go:embed example-config.yaml
var ExampleConfig string
func (tc *TraqConnector) GetConfig() (string, any, up.Upgrader) {
	return ExampleConfig, &tc.Config, up.SimpleUpgrader(func(helper up.Helper) {
		helper.Copy(up.Str, "api_url")
		helper.Copy(up.Bool, "some_flag")
		helper.Copy(up.Int|up.Null, "some_limit")
	})
}  // up = "go.mau.fi/util/configupgrade"; types used in repo: up.Str, up.Bool, up.Int, up.List, up.Map, up.Null (OR-able)
```
`up.SimpleUpgrader(fn)` is used as an `Upgrader` in `mxmain/main.go:212`. `bridgeconfig/upgrade.go` shows the `&up.StructUpgrader{SimpleUpgrader: ..., Blocks: ...}` variant for blank-line blocks.

Relevant base keys (`bridgev2/matrix/mxmain/example-config.yaml`, parsed into `br.Config` = `*bridgeconfig.BridgeConfig`): `bridge.command_prefix`, `bridge.split_portals`, `bridge.bridge_notices: false` (m.notice not bridged → `ErrIgnoringMNotice`), `bridge.mute_only_on_create: true`, `bridge.tag_only_on_create: true`, `bridge.only_bridge_tags`, `bridge.portal_event_buffer: 64`, `bridge.deduplicate_matrix_messages`, `matrix.message_status_events: false`, `matrix.message_error_notices: true`.

---

## 10. Double puppeting

What `EventSender{IsFromMe: true}` does (`getIntentAndUserMXIDFor`, `bridgev2/portal.go`):
- The event is sent with `source.User.DoublePuppet(ctx)`, i.e. as the real Matrix user.
- If no double puppet is available, it **falls back to the ghost of `Sender`**, or to the bridge bot if `Sender` is empty. So always set `Sender` (your own traQ user ID) together with `IsFromMe`.
- `SenderLogin` (other login's ID, non-receiver portals only) does the same for events received through another login.

`User.DoublePuppet` (`bridgev2/user.go`) lazily creates an intent via `Matrix.NewUserIntent(ctx, user.MXID, user.AccessToken)`. Ways to enable it:
- **Automatic:** config `double_puppet.secrets: {"example.com": "as_token:<token>"}`, see the example config and docs link <https://docs.mau.fi/bridges/general/double-puppeting.html>. Other servers go in `double_puppet.servers`, plus `allow_discovery`.
- **Manual:** the `login-matrix <access token>` bot command (`bridgev2/matrix/cmddoublepuppet.go`; also `ping-matrix`, `logout-matrix`). `User.LoginDoublePuppet(ctx, token) error`.

Own read state: queue `simplevent.Receipt` with `Type: bridgev2.RemoteEventReadReceipt`, `Sender: bridgev2.EventSender{IsFromMe: true, Sender: myID}` and `LastTarget` (or `Targets`, or `ReadUpTo`).

The bridge resolves the target message via `GetLastPartByID`, falling back to `GetLastNonFakePartAtOrBeforeTime(ReadUpTo)`, and calls `intent.MarkRead`. Without a target it is ignored (unless stream order is supported). Without a double puppet the receipt is sent as the ghost.

Mute: `ChatInfo.UserLocal = &bridgev2.UserLocalPortalInfo{MutedUntil: &t}`. The bridge applies it only with a double puppet (`dp.MuteRoom`). It is applied on room creation (only if `t` is in the future). On later `UpdateInfo` calls (ChatResync / ChatInfoChange) it is applied **only if `bridge.mute_only_on_create: false`** (default true). Use `event.MutedForever` for an indefinite mute and `bridgev2.Unmuted` to unmute.

---

## 11. Backfill

```go
type FetchMessagesParams struct {
	Portal         *Portal
	ThreadRoot     networkid.MessageID
	Forward        bool                     // true: newer than AnchorMessage
	AnchorMessage  *database.Message        // oldest known (backward) / newest known (forward); nil if room empty
	Cursor         networkid.PaginationCursor // from previous backward call; NOT present for Forward
	Count          int                      // forward: max_initial_messages (empty room) or max_catchup_messages; backward: queue.batch_size
	AllowSlowFetch bool
	BundledData    any                      // from ChatResync.BundledBackfillData
	Task           *database.BackfillTask   // queued backward backfill
}
type FetchMessagesResponse struct {
	Messages []*BackfillMessage  // chronological, oldest → newest
	Cursor   networkid.PaginationCursor
	HasMore  bool                // REQUIRED; false stops further backward calls
	Forward  bool                // batch contains new messages (Cursor/HasMore ignored)
	MarkRead bool                // mark read after forward backfill / first batch (needs double puppet)
	ReadUpTo time.Time           // alternative to MarkRead
	MoreRequiresSlowFetch, Pending, AggressiveDeduplication bool
	ApproxProgress float64; ApproxRemainingCount, ApproxTotalCount int; CompleteCallback func()
}
type BackfillMessage struct {
	*ConvertedMessage
	Sender EventSender; ID networkid.MessageID; TxnID networkid.TransactionID; Timestamp time.Time; StreamOrder int64
	Reactions []*BackfillReaction; ShouldBackfillThread bool; LastThreadMessage networkid.MessageID
}
type BackfillReaction struct {
	TargetPart *networkid.PartID   // nil = first part
	Timestamp  time.Time           // zero = fake ts just after message
	Sender EventSender; EmojiID networkid.EmojiID; Emoji string; ExtraContent map[string]any; DBMetadata any
}
```
Note that `BackfillMessage` carries an already-converted message: convert and upload media inside `FetchMessages`, using `params.Portal` and an intent from `portal.GetIntentFor(ctx, sender, login, bridgev2.RemoteEventMessage)`.

Config (`bridgeconfig/backfill.go`) is the **top-level** `backfill:` section; mxmain copies it into `cfg.Bridge.Backfill`:
```yaml
backfill:
    enabled: false              # master switch
    max_initial_messages: 50    # forward fetch when room is created (requires ChatInfo.CanBackfill)
    max_catchup_messages: 500   # forward catch-up on ChatResync when room has messages
    unread_hours_threshold: 720
    threads: { max_initial_messages: 50 }
    queue:                      # BACKWARD backfill — only with Beeper batch sending
        enabled: false
        manual: false
        batch_size: 100
        batch_delay: 20
        max_batches: -1
        max_batches_override: {}
```
How backfill triggers:
- **Room creation** (`createMatrixRoomInLoop`): if `info.CanBackfill`, a backfill task is upserted. If `backfill.enabled` is also set, `doForwardBackfill(..., nil, bundle)` runs with `Count = max_initial_messages`.
- **`ChatResync`** (`handleRemoteChatResync`): if `backfill.enabled` and the event implements `RemoteChatResyncBackfill`, the bridge takes `latest = GetLastPartAtOrBeforeTime(now+10s)` and calls `CheckNeedsBackfill(ctx, latest)`.
  `simplevent.ChatResync` default: `CheckNeedsBackfillFunc` if set; else `!LatestMessageTS.IsZero()` when the room is empty; else `LatestMessageTS.After(latest.Timestamp)`. If true → `FetchMessages{Forward: true, AnchorMessage: latest, Count: max_catchup_messages}`; returned messages older than the anchor are cut off.
- Only one forward backfill per portal at a time (`TryLock`). Typical startup catch-up: in `Connect`, for each channel queue a `ChatResync{EventMeta{Type: RemoteEventChatResync, PortalKey, CreatePortal: true}, LatestMessageTS: lastActivity}`.
- **Backward queue:** requires `Matrix.GetCapabilities().BatchSending` (Beeper only). `RunBackfillQueue` warns and returns on standard servers. Forward backfill on standard servers sends normal events with timestamp massaging.

---

## 12. Bridge states — `bridgev2/status/bridgestate.go`, `bridgev2/bridgestate.go`
```go
const ( // all BridgeStateEvent (string)
	StateStarting = "STARTING"; StateUnconfigured = "UNCONFIGURED"; StateRunning = "RUNNING"; StateBridgeUnreachable = "BRIDGE_UNREACHABLE"
	StateConnecting = "CONNECTING"; StateBackfilling = "BACKFILLING"; StateConnected = "CONNECTED"; StateTransientDisconnect = "TRANSIENT_DISCONNECT"
	StateBadCredentials = "BAD_CREDENTIALS"; StateUnknownError = "UNKNOWN_ERROR"; StateLoggedOut = "LOGGED_OUT" )
type BridgeStateErrorCode string
var BridgeStateHumanErrors = make(BridgeStateErrorMap) // map[BridgeStateErrorCode]string; .Update(map) to register human texts
type BridgeState struct {
	StateEvent BridgeStateEvent `json:"state_event"`; Timestamp jsontime.Unix; TTL int; Source string
	Error      BridgeStateErrorCode `json:"error,omitempty"`   // e.g. "traq-invalid-token"
	Message    string               `json:"message,omitempty"` // human text
	UserAction BridgeStateUserAction                            // UserActionOpenNative / UserActionRelogin / UserActionRestart
	UserID id.UserID; RemoteID networkid.UserLoginID; RemoteName string; RemoteProfile RemoteProfile // filled by Fill(login)
	Version any; Reason string; Info map[string]any
	BlockAutoReconnect bool `json:"-"`
}
func (bsq *BridgeStateQueue) Send(state status.BridgeState)  // login.BridgeState.Send(status.BridgeState{StateEvent: status.StateConnected})
func (bsq *BridgeStateQueue) GetPrev() status.BridgeState
```
Behaviour:
- `Send` fills `RemoteID`, `RemoteName` and `RemoteProfile` from the login. It never blocks: the queue holds 8, then drops.
- `UNKNOWN_ERROR` triggers an automatic `Disconnect` + recreate client (`LoadUserLogin`) + `Connect`, subject to `bridge.unknown_error_auto_reconnect` / `unknown_error_max_auto_reconnects` and `BlockAutoReconnect`.
- `TRANSIENT_DISCONNECT` notices are debounced (`transient_state_debounce`). State notices go to the management room (`bridge_status_notices`) as "State update for <RemoteName>: `STATE` (`error`): message".

---

## 13. Message status errors — `bridgev2/errors.go`, `bridgev2/messagestatus.go`
```go
type MessageStatus struct {
	Step status.MessageCheckpointStep; RetryNum int
	Status      event.MessageStatus        // MessageStatusSuccess "SUCCESS", MessageStatusPending, MessageStatusRetriable "FAIL_RETRIABLE", MessageStatusFail "FAIL_PERMANENT"
	ErrorReason event.MessageStatusReason  // MessageStatusGenericError, MessageStatusUnsupported, MessageStatusTooOld, MessageStatusNetworkError, MessageStatusNoPermission, MessageStatusBridgeUnavailable
	DeliveredTo []id.UserID; InternalError error; Message string
	DisappearingTimer *event.BeeperDisappearingTimer
	ErrorAsMessage, IsCertain, SendNotice, DisableMSS bool
}  // implements error (Error/Unwrap/Is)
func WrapErrorInStatus(err error) MessageStatus // default Status=FAIL_RETRIABLE, Reason=generic, SendNotice=false; uses HumanError() text if err implements it
func (ms MessageStatus) WithSendNotice(send bool) MessageStatus
func (ms MessageStatus) WithIsCertain(certain bool) MessageStatus
func (ms MessageStatus) WithMessage(msg string) MessageStatus     // human text; clears ErrorAsMessage
func (ms MessageStatus) WithStep(step status.MessageCheckpointStep) MessageStatus
func (ms MessageStatus) WithStatus(status event.MessageStatus) MessageStatus
func (ms MessageStatus) WithErrorReason(reason event.MessageStatusReason) MessageStatus
func (ms MessageStatus) WithErrorAsMessage() MessageStatus        // show InternalError text to user
type HumanError interface { error; HumanError() string }
```
Ready-made errors (`var ... error = WrapErrorInStatus(...)...`):
- **Notice sent** (`SendNotice` true): `ErrUnsupportedMessageType`, `ErrUnsupportedMediaType`, `ErrMediaTooLarge`, `ErrMediaDownloadFailed`, `ErrMediaReuploadFailed`, `ErrMediaConvertFailed`, `ErrCaptionsNotAllowed`, `ErrLocationMessagesNotAllowed`, `ErrDatabaseError`, `ErrPollsNotSupported`, `ErrPanicInEventHandler`.
- **No notice:** `ErrEditsNotSupported`, `ErrEditsNotSupportedInPortal`, `ErrEditTargetTooOld`, `ErrEditTargetTooManyEdits`, `ErrReactionsNotSupported`, `ErrRedactionsNotSupported`, `ErrTargetMessageNotFound`, `ErrIgnoringMNotice`, `ErrRoomMetadataNotSupported`.
- **Plain errors:** `ErrIgnoringRemoteEvent` (drop remote event silently), `ErrNotLoggedIn`, `ErrNoStatus` ("omit message status"), `ErrFailedToGetIntent`.

Flow: a non-nil error from `HandleMatrixMessage`/`HandleMatrixEdit`/`HandleMatrixReaction`/`...Remove` becomes `EventHandlingResultFailed.WithMSSError(err)`, then `portal.sendErrorStatus`, which runs `WrapErrorInStatus` and `Matrix.SendMessageStatus`. That sends a checkpoint, plus an MSS event if `matrix.message_status_events`.

**A notice is only sent if `ms.SendNotice` is true** and `matrix.message_error_notices` is enabled (default true), and the status is FAIL_*. Notice text: "⚠️ Your message was not bridged: <msg>" ("may not have been" if `!IsCertain`), sent as a reply.

So a plain `fmt.Errorf` gives **no user-visible notice**. Return `bridgev2.WrapErrorInStatus(err).WithSendNotice(true).WithIsCertain(true).WithErrorAsMessage()`, or `.WithMessage("traQ rejected the message")`, or `fmt.Errorf("%w: too long", bridgev2.ErrUnsupportedMessageType)` (wrapping keeps the flags via `errors.As`).

---

## Gotchas
- **Portal event loop is serial per portal.** Matrix events, remote events and room creation share one queue (`portal.eventLoop`, buffer `portal_event_buffer`). A slow `HandleMatrixMessage` blocks incoming traQ events for that channel.
  Upside: an echo of your own message arriving while the send runs is handled after it and deduped by `MessageID` against the saved `resp.DB` row. If the echo can arrive *before* you know the ID, use `AddPendingToIgnore(txnID)` + `RemovePending`, or `AddPendingToSave` + `Pending: true`, with `Message[T].TransactionID`.
- **Panics** in portal event handlers and command handlers are recovered. Portal panics send `ErrPanicInEventHandler`. Command panics send "Handling your command panicked". A wrong `EventMeta.Type` → interface-assertion panic.
- **LoadUserLogin runs under `br.cacheLock`** (also inside `NewLogin`). Do not call `br.Get*` there (deadlock risk) and do no IO: just construct the client.
- **The bridge never calls `Connect` after login**; you must, with a non-request context. `Connect` has no error return: report via `BridgeState.Send`.
- **`QueueRemoteEvent` before the room exists:** events without `CreatePortal: true` are silently dropped ("Dropping event as portal doesn't exist"). `GetPortalByKey` creates DB rows for any key you pass. Use `GetExistingPortalByKey` for lookups that shouldn't create rows.
- **`GetChatInfo` for room creation must include `Members`.** A nil `Members` makes the bridge call `GetChatInfo` again. Include yourself (`IsFromMe: true`) in `MemberMap`. Set `CanBackfill: true` or no initial backfill. For DMs set `Type: new(database.RoomTypeDM)` (Go 1.26 `new(expr)`; a const can't be addressed with `&`) and `OtherUserID`.
- **Reply/edit capabilities gate the connector's input:** `Reply` not Partial → `msg.ReplyTo == nil`; `Edit` not Partial → `HandleMatrixEdit` is never called. `MaxTextLength` is not enforced by the bridge.
- **`m.notice` from Matrix is dropped by default** (`bridge.bridge_notices: false`).
- `database.Message.ID`/`SenderID` must be set by you in `MatrixMessageResponse.DB`; returning `DB: nil` logs an error and saves nothing.
- **Reactions:** traQ users can add multiple stamps, so use a non-empty `EmojiID` (stamp ID). With `EmojiID == ""`, the bridge treats a second reaction from the same user as an override. `simplevent.Reaction` serves both add and remove; only `Type` differs.
- **`Avatar.ID` drives change detection:** same ID → `Get` is not called again. Use a stable file ID/hash.
- **`IsFromMe` without double puppeting** falls back to the `Sender` ghost, so always fill `Sender`. Mute/read sync needs a double puppet. Mute after creation needs `mute_only_on_create: false`.
- **`HandleMatrixViewingChat` is never called** on standard (non-Beeper) homeservers. The backward backfill queue also needs Beeper batch sending; only forward backfill works on Synapse/Conduit etc.
- **The built-in `search` command exists**; `AddHandler` with the same name silently replaces it. mxmain registers `version` after `Network.Init`, so you can't override `version` from `Init`.
- **`ce.Reply` uses `fmt.Sprintf` only when args are given.** It always replaces `$cmdprefix` and renders markdown, so escape user-provided text with `format.EscapeMarkdown`.
- **Login user_input is collected one message per field.** While a login is pending, a message whose first word matches a command name runs that command instead (`handleCommand` checks handlers first). `password`/`token` fields are redacted by the bot.
- **`format.RenderMarkdown` returns a value** (`event.MessageEventContent`); take its address for `ConvertedMessagePart.Content`. Set `Type: event.EventMessage` on parts.
- **`GetName()` and `GetConfig()` run before config load** (`-e`, upgrader). They must not dereference the config. `GetDBMetaTypes()` runs before `Init`.
- **The DB upgrade for bridgev2 tables runs in `StartConnectors` before `Network.Start`.** Run your child-DB `Upgrade` in `Start` and return `bridgev2.DBUpgradeError` on failure.
- **`bridgev2.ErrIgnoringRemoteEvent` from a Convert func = silent drop.** Other errors post a generic "An error occurred while processing an incoming message" notice in the room.
