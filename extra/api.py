
def extend_api(api_bp):

    import flask_login
    from flask import render_template, request
    from flask import current_app as app
    import os

    from emhub.blueprints.api import handle_session, handle_user
    from emhub.utils import send_json_data, send_error

    @api_bp.route('/create_session_extended', methods=['POST'])
    @flask_login.login_required
    def create_session_extended():
        def _create_session_extended(**args):
            """ Add a new session row. """
            tasks = args.pop('tasks', [])

            b = app.dm.get_booking_by(id=int(args['booking_id']))
            args['resource_id'] = b.resource.id
            args['operator_id'] = b.owner.id if b.operator is None else b.operator.id

            if 'start' not in args:
                args['start'] = b.start

            if 'status' not in args:
                args['status'] = 'active'

            # If the session name is not provided,
            # it will be picked from the amount of sessions the owner has for today
            if 'name' not in args:
                from datetime import date
                today = date.today()
                session_name = f'{today.year}_{today.month}_{today.day}_{b.owner.name.replace(" ", "_")}'
                n_sessions = app.dm.get_sessions()
                session_name = f'{session_name}_{len(n_sessions) + 1}'
                args['name'] = session_name

            s = app.dm.get_session_by(name=args['name'])
            if s is not None:
                raise Exception("Session name already exist, "
                                "choose a different one.")

            session = app.dm.Session(**args)
            app.dm._db_session.add(session)
            app.dm.commit()
            app.dm.log('operation', 'create_%s' % app.dm.Session.__name__, attrs=app.dm.json_from_dict(args))

            for args, worker in tasks:
                args_pretty = {}
                args_pretty['session_id'] = session.id
                args_pretty['session_name'] = session.name
                args_pretty['action'] = args
                # Update some values for the task
                task = {
                    'name': 'session',
                    'args': args_pretty
                }
                app.dm.get_worker_stream(worker).create_task(task)

            return session

        return handle_session(_create_session_extended)


    @api_bp.route('/send_data_sharing_mail', methods=['POST'])
    @flask_login.login_required
    def send_data_sharing_mail():
        def _send_data_sharing_mail(**args):
            session = app.dm.get_session_by(id=args['session_id'])
            extra = dict(session.extra)
            booking = app.dm.get_booking_by(id=session.booking_id)

            if booking:
                user = app.dm.get_user_by(email=booking.owner.email)

                raw = extra['raw']
                if raw.get('irods', {}).get('linux', '') and raw.get('irods', {}).get('windows', ''):
                    app.mm.send_mail(
                        [booking.owner.email],
                        f"emhub: Download your the raw data of your session {session.name}",
                        render_template('email/download_data.txt',
                                        user=user,
                                        session=session.name,
                                        data_type='raw',
                                        linux_cmd=raw['irods']['linux'],
                                        windows_cmd=raw['irods']['windows']))

                otf = extra['otf']
                if otf.get('irods', {}).get('linux', '') and otf.get('irods', {}).get('windows', ''):
                    app.mm.send_mail(
                        [booking.owner.email],
                        f"emhub: Download your the Scipion data of your session {session.name}",
                        render_template('email/download_data.txt',
                                        user=user,
                                        session=session.name,
                                        data_type='Scipion',
                                        linux_cmd=otf['irods']['linux'],
                                        windows_cmd=otf['irods']['windows']))

            return session

        return handle_session(_send_data_sharing_mail)


    @api_bp.route('/register_user_extended', methods=['POST'])
    @flask_login.login_required
    def register_user_extended():
        def _register_user_extended(**attrs):
            email = attrs['email'].strip()
            user = app.dm.create_user(
                username=email,
                email=email,
                phone='',
                password=os.urandom(24).hex(),
                name=attrs['name'],
                roles=attrs['roles'],
                pi_id=attrs['pi_id'],
                status='active',
                extra={'laboratory': attrs['laboratory'],
                       'project_nickname': attrs['project_nickname'],
                       'funding_account': attrs['funding_account'],
                       'funding_eu': attrs['funding_eu'],
                       'collaborators': attrs['collaborators']}
            )

            if app.mm:
                app.mm.send_mail(
                    [user.email],
                    "emhub: New account registered",
                    render_template('email/account_registered.txt', user=user))
            return user

        return handle_user(_register_user_extended)

    @api_bp.route('/update_user_form_extended', methods=['POST'])
    @flask_login.login_required
    def update_user_form_extended():
        try:
            f = request.form
            attrs = {'id': f['user-id'],
                     'name': f['user-name'],
                     'phone': f['user-phone'],
                     'status': f['user-status-select'],
                     'extra': {'laboratory': f['laboratory-name'],
                               'project_nickname': f['project-nickname'],
                               'funding_account': f['funding-account'],
                               'funding_eu': f['funding-eu'] == "true",
                               'collaborators': f['collaborators']}
                     }

            roles = [v.replace('role-', '') for v in f if v.startswith('role-')]
            if roles:
                attrs['roles'] = roles

            if 'user-pi-select' in f:
                pi_id = int(f['user-pi-select'])
                if pi_id:
                    attrs['pi_id'] = pi_id
                # TODO: Validate if a user is not longer PI
                # check that there are not other users referencing this one as pi
                # still this will not be a very common case

            password = f['user-password'].strip()
            if password:
                attrs['password'] = password

            if 'user-profile-image' in request.files:
                profile_image = request.files['user-profile-image']

                if profile_image.filename:
                    _, ext = os.path.splitext(profile_image.filename)

                    if ext.lstrip(".").upper() not in app.config["ALLOWED_IMAGE_EXTENSIONS"]:
                        return send_error("Image format %s is not allowed!" % ext.upper())
                    else:
                        image_name = 'profile-image-%06d%s' % (int(f['user-id']), ext)
                        image_path = os.path.join(app.config['USER_IMAGES'], image_name)
                        profile_image.save(image_path)
                        attrs['profile_image'] = image_name

            app.dm.update_user(**attrs)

            return send_json_data({'user': attrs})

        except Exception as e:
            print(e)
            return send_error('ERROR from Server: %s' % e)