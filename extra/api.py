
def extend_api(api_bp):

    import flask_login
    from flask import current_app as app

    from emhub.blueprints.api import handle_session

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
                n_sessions_owned_today = app.dm.get_sessions(condition=f'name LIKE "{session_name}%"')
                session_name = f'{session_name}_{len(n_sessions_owned_today) + 1}'
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