package io.ltirom.server.toolchain

import io.ltirom.server.model.RunPurpose
import kotlinx.serialization.Serializable

@Serializable
public enum class ToolchainState {
    UNINITIALIZED,
    READY,
    RECOVERY_REQUIRED
}

public interface ToolchainStateMachine {
    public val state: ToolchainState
    public val activeInstallId: String?
    public val previousInstallId: String?
    public val recoveryRequiredBy: String?

    public fun canAdmit(purpose: RunPurpose): Boolean
}

public class MutableToolchainStateMachine(
    initialState: ToolchainState = ToolchainState.READY,
    initialActive: String? = null,
    initialPrevious: String? = null,
    initialRecoveryRequiredBy: String? = null
) : ToolchainStateMachine {

    @Volatile
    private var _state: ToolchainState = initialState

    @Volatile
    private var _activeInstallId: String? = initialActive

    @Volatile
    private var _previousInstallId: String? = initialPrevious

    @Volatile
    private var _recoveryRequiredBy: String? = initialRecoveryRequiredBy

    override val state: ToolchainState get() = _state
    override val activeInstallId: String? get() = _activeInstallId
    override val previousInstallId: String? get() = _previousInstallId
    override val recoveryRequiredBy: String? get() = _recoveryRequiredBy

    public fun update(
        newState: ToolchainState,
        active: String? = _activeInstallId,
        previous: String? = _previousInstallId,
        recoveryRequiredBy: String? = _recoveryRequiredBy
    ) {
        _state = newState
        _activeInstallId = active
        _previousInstallId = previous
        _recoveryRequiredBy = recoveryRequiredBy
    }

    override fun canAdmit(purpose: RunPurpose): Boolean = when (purpose) {
        RunPurpose.SETUP -> true
        RunPurpose.WORKSPACE -> _state == ToolchainState.READY
    }
}

public class JournalToolchainStateMachine(
    private val journal: ToolchainJournal
) : ToolchainStateMachine {
    override val state: ToolchainState get() = journal.fold().state
    override val activeInstallId: String? get() = journal.fold().activeInstallId
    override val previousInstallId: String? get() = journal.fold().previousInstallId
    override val recoveryRequiredBy: String? get() = journal.fold().recoveryRequiredBy

    override fun canAdmit(purpose: RunPurpose): Boolean = when (purpose) {
        RunPurpose.SETUP -> true
        RunPurpose.WORKSPACE -> state == ToolchainState.READY
    }
}

public fun ToolchainStateMachine(journal: ToolchainJournal): ToolchainStateMachine =
    JournalToolchainStateMachine(journal)
