"""
AGPARS Onboarding Contract Test

Tests for T028: /start onboarding workflow (role-based auth).
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from packages.storage.models import UserRole, WorkspaceType


class TestOnboardingFlow:
    """Tests for /start onboarding handler with role-based auth."""

    def _make_update(self, user_id=12345, chat_id=12345, chat_type="private",
                     full_name="Test User", username="testuser", chat_title=None):
        """Create a mock Update object."""
        update = MagicMock()
        update.effective_user.id = user_id
        update.effective_user.full_name = full_name
        update.effective_user.username = username
        update.effective_chat.id = chat_id
        update.effective_chat.type = chat_type
        update.effective_chat.title = chat_title
        update.message.reply_text = AsyncMock()
        return update

    @pytest.mark.asyncio
    @patch("services.bot.handlers.onboard.send_menu_message", new_callable=AsyncMock)
    @patch("services.bot.handlers.onboard.build_main_menu_keyboard")
    @patch("services.bot.handlers.onboard.get_subscriptions_for_workspace", return_value=[])
    @patch("services.bot.handlers.onboard.create_subscription", return_value=1)
    @patch("services.bot.handlers.onboard.get_or_create_workspace")
    @patch("services.bot.handlers.onboard.get_or_create_user")
    @patch("services.bot.handlers.onboard.get_settings")
    async def test_start_authorized_creates_workspace(
        self, mock_settings, mock_get_user, mock_get_ws,
        mock_create_sub, mock_get_subs, mock_keyboard, mock_send,
    ):
        """T028: /start by authorized user creates workspace + default sub."""
        mock_settings.return_value.telegram.admin_user_id = 99999

        mock_get_user.return_value = {
            "id": 1, "tg_user_id": 12345, "role": "regular",
            "tg_username": "testuser", "full_name": "Test User",
        }
        mock_get_ws.return_value = {
            "id": 1, "type": "personal", "tg_chat_id": 12345, "title": "Test User",
        }

        from services.bot.handlers.onboard import handle_start
        update = self._make_update()
        await handle_start(update, MagicMock())

        mock_get_user.assert_called_once()
        mock_get_ws.assert_called_once()
        mock_create_sub.assert_called_once()
        mock_send.assert_called_once()

    @pytest.mark.asyncio
    @patch("services.bot.handlers.onboard.send_menu_message", new_callable=AsyncMock)
    @patch("services.bot.handlers.onboard.get_or_create_user")
    @patch("services.bot.handlers.onboard.get_settings")
    async def test_start_unauthorized_shows_request_button(
        self, mock_settings, mock_get_user, mock_send,
    ):
        """T028: /start by unauthorized user shows 'Request Access' button."""
        mock_settings.return_value.telegram.admin_user_id = 99999

        mock_get_user.return_value = {
            "id": 1, "tg_user_id": 12345, "role": "unauthorized",
        }

        from services.bot.handlers.onboard import handle_start
        update = self._make_update()
        await handle_start(update, MagicMock())

        mock_send.assert_called_once()
        call_args = mock_send.call_args
        text = call_args[0][2]
        assert "нет доступа" in text

    @pytest.mark.asyncio
    @patch("services.bot.handlers.onboard.send_menu_message", new_callable=AsyncMock)
    @patch("services.bot.handlers.onboard.build_main_menu_keyboard")
    @patch("services.bot.handlers.onboard.get_subscriptions_for_workspace")
    @patch("services.bot.handlers.onboard.get_or_create_workspace")
    @patch("services.bot.handlers.onboard.get_or_create_user")
    @patch("services.bot.handlers.onboard.get_settings")
    async def test_start_existing_workspace_no_duplicate_sub(
        self, mock_settings, mock_get_user, mock_get_ws,
        mock_get_subs, mock_keyboard, mock_send,
    ):
        """T028: /start with existing subs doesn't create duplicate."""
        mock_settings.return_value.telegram.admin_user_id = 99999

        mock_get_user.return_value = {
            "id": 1, "tg_user_id": 12345, "role": "regular",
        }
        mock_get_ws.return_value = {
            "id": 1, "type": "personal", "tg_chat_id": 12345, "title": "Test User",
        }
        mock_get_subs.return_value = [
            {"id": 1, "name": "Existing", "filters": {}},
        ]

        from services.bot.handlers.onboard import handle_start
        update = self._make_update()
        await handle_start(update, MagicMock())

        # Check the welcome text mentions existing subscription count
        call_args = mock_send.call_args
        text = call_args[0][2]
        assert "1 subscription" in text

    @pytest.mark.asyncio
    @patch("services.bot.handlers.onboard.send_menu_message", new_callable=AsyncMock)
    @patch("services.bot.handlers.onboard.build_main_menu_keyboard")
    @patch("services.bot.handlers.onboard.get_subscriptions_for_workspace", return_value=[])
    @patch("services.bot.handlers.onboard.create_subscription", return_value=1)
    @patch("services.bot.handlers.onboard.get_or_create_workspace")
    @patch("services.bot.handlers.onboard.get_or_create_user")
    @patch("services.bot.handlers.onboard.get_settings")
    async def test_start_group_chat(
        self, mock_settings, mock_get_user, mock_get_ws,
        mock_create_sub, mock_get_subs, mock_keyboard, mock_send,
    ):
        """T028: /start in group chat creates GROUP workspace."""
        mock_settings.return_value.telegram.admin_user_id = 99999

        mock_get_user.return_value = {
            "id": 1, "tg_user_id": 12345, "role": "admin",
        }
        mock_get_ws.return_value = {
            "id": 2, "type": "group", "tg_chat_id": -100123, "title": "Test Group",
        }

        from services.bot.handlers.onboard import handle_start
        update = self._make_update(chat_id=-100123, chat_type="supergroup", chat_title="Test Group")
        await handle_start(update, MagicMock())

        call_args = mock_get_ws.call_args
        assert call_args[1]["workspace_type"] == WorkspaceType.GROUP

    @pytest.mark.asyncio
    @patch("services.bot.handlers.onboard.send_menu_message", new_callable=AsyncMock)
    @patch("services.bot.handlers.onboard.build_main_menu_keyboard")
    @patch("services.bot.handlers.onboard.get_subscriptions_for_workspace", return_value=[])
    @patch("services.bot.handlers.onboard.create_subscription", return_value=1)
    @patch("services.bot.handlers.onboard.get_or_create_workspace")
    @patch("services.bot.handlers.onboard.get_or_create_user")
    @patch("services.bot.handlers.onboard.get_settings")
    async def test_start_admin_auto_promote(
        self, mock_settings, mock_get_user, mock_get_ws,
        mock_create_sub, mock_get_subs, mock_keyboard, mock_send,
    ):
        """T028: /start auto-promotes configured admin_user_id."""
        mock_settings.return_value.telegram.admin_user_id = 12345  # matches user

        mock_get_user.return_value = {
            "id": 1, "tg_user_id": 12345, "role": "admin",
        }
        mock_get_ws.return_value = {
            "id": 1, "type": "personal", "tg_chat_id": 12345, "title": "Admin",
        }

        from services.bot.handlers.onboard import handle_start
        update = self._make_update()
        await handle_start(update, MagicMock())

        # get_or_create_user should be called with admin default role
        call_args = mock_get_user.call_args
        assert call_args[1]["default_role"] == UserRole.ADMIN


class TestHelpHandler:
    """Tests for /help handler."""

    @pytest.mark.asyncio
    @patch("services.bot.handlers.onboard.send_menu_message", new_callable=AsyncMock)
    @patch("services.bot.handlers.onboard.build_main_menu_keyboard")
    @patch("services.bot.handlers.onboard.get_user_by_tg_id")
    async def test_help_shows_menu(self, mock_get_user, mock_keyboard, mock_send):
        """Help shows menu keyboard."""
        mock_get_user.return_value = {"role": "regular"}

        from services.bot.handlers.onboard import handle_help
        update = MagicMock()
        update.effective_chat.id = 12345
        update.effective_user.id = 12345

        await handle_help(update, MagicMock())

        mock_send.assert_called_once()
        call_args = mock_send.call_args
        text = call_args[0][2]
        assert "AGPARS" in text
