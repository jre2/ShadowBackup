import cPickle as pickle
import gzip
import os
import os.path
from   pprint import pprint as pp
import sys
import time
import uuid
import xattr

#TODO: options parsing
#maybe: merge DB/shadows???

realRoot = '/.arc/1'
shadowRoot = os.path.join( realRoot, '.shadow' )
shadowDbPath = os.path.join( shadowRoot, 'shadow.db' )

SKIP_SHADOW_CHECK = False

def memoize( f ):
    class memodict(dict):
        def __missing__( self, k ):
            r = self[k] = f(k)
            return r
    return memodict().__getitem__

# replace os.stat() with memoizing version
if not isinstance( os.stat.__self__, dict ):
    _os_stat_bak = os.stat
    @memoize
    def _fast_stat( f ): return _os_stat_bak( f )
    os.stat = _fast_stat

def loadDB(): return pickle.load( gzip.open( shadowDbPath, 'rb' ) )
def saveDB( db ): return pickle.dump( db, gzip.open( shadowDbPath, 'wb' ) )

def iterFiles( root, skipDir=None ):
    for r, _, fs in os.walk( root ):
        if skipDir and r == skipDir: continue
        for f in fs:
            yield os.path.join( r, f )

def getUUIDFromXAttr( p ):
    try:
        return xattr.getxattr( p, 'user.uuid' )
    except IOError:
        return None

def getUUIDFromShadow( p ):
    if SKIP_SHADOW_CHECK: return None
    for s in iterFiles( shadowRoot ):
        if os.path.samefile( p, s ):
            return os.path.basename( s )
    return None

def createNewUUID( db ):
    u = str( uuid.uuid1() )
    if u in db: raise RuntimeError( 'Duplicate UUID generated! %s -> %s' % u, db[u] )
    return u

def setUUIDInXAttr( p, uuid, debug=False ):
    if debug: print '    Setting xattr'
    xattr.setxattr( p, 'user.uuid', uuid )

def createShadow( p, uuid, debug=False ):
    s = os.path.join( shadowRoot, uuid )
    if debug: print '    Linking %s >>> %s' % ( p, s )
    os.link( p, s )

def updateShadow( strict=True, debug=False ):
    '''Based on real'''
    # Setup
    db = {}
    t_last = time.time()
    if not os.path.exists( shadowRoot ): os.makedirs( shadowRoot )

    for i,p in enumerate( iterFiles( realRoot, skipDir=shadowRoot ) ):
        #if debug: print 'Processing', i, p
        uuid = getUUIDFromXAttr( p ) or getUUIDFromShadow( p )
        if not uuid:
            if debug: print '  Adding', i, p
            uuid = createNewUUID( db )
            setUUIDInXAttr( p, uuid )
            createShadow( p, uuid )
        db[ uuid ] = os.path.relpath( p, realRoot )

        # Strict mode checks, for consistency
        if strict:
            if not os.path.exists( os.path.join( shadowRoot, uuid ) ):
                print '  STRICT shadow', p
                createShadow( p, uuid )
            if not getUUIDFromXAttr( p ):
                print '  STRICT xattr', p
                setUUIDInXAttr( p, uuid )

        # Timing info
        if i>0 and i % 1000 == 0:
            t_cur = time.time()
            print 'Processed 1000 in', t_cur-t_last
            t_last = t_cur

    # Save db
    saveDB( db )
    return db

def restoreReal( debug=False, force=False, dry_run=True, removePrefix=None ):
    '''Based on DB'''
    db = loadDB()
    num_restored = 0
    t_last = time.time()

    for i,(uuid, rel_p) in enumerate( db.iteritems() ):
        #if debug: print 'Processing', i, uuid
        if removePrefix: rel_p = os.path.relpath( rel_p, removePrefix )
        p = os.path.join( realRoot, rel_p )
        if force or not os.path.exists( p ):
            if debug: print '  Restoring', p
            shadow = os.path.join( shadowRoot, uuid )
            parent = os.path.dirname( p )
            if not dry_run:
                if not os.path.exists( parent ):
                    os.makedirs( parent )
                os.link( shadow, p )
                setUUIDInXAttr( p, uuid )
            num_restored += 1

        # Timing info
        if i>0 and i % 1000 == 0:
            t_cur = time.time()
            print 'Processed 1000 in', t_cur-t_last
            t_last = t_cur

    print 'Restored %d files' % num_restored

def cleanShadow( debug=False, dry_run=True ):
    '''Based on shadow'''
    db = loadDB()
    num_cleaned = 0
    t_last = time.time()

    for i,s in enumerate( iterFiles( shadowRoot ) ):
        #if debug: print 'Processing', i, s
        uuid = os.path.basename( s )
        if uuid == 'shadow.db': continue
        p = os.path.join( realRoot, db[uuid] ) if uuid in db else None
        if not p or not os.path.exists( p ):
            if debug: print '  Cleaning', p or uuid
            if not dry_run: os.remove( s )
            num_cleaned += 1

        # Timing info
        if i>0 and i % 1000 == 0:
            t_cur = time.time()
            print 'Processed 1000 in', t_cur-t_last
            t_last = t_cur

    print 'Cleaned %d files' % num_cleaned

def main():
    updateShadow( debug=True )
    #restoreReal( dry_run=True, debug=True )
    #cleanShadow( dry_run=False, debug=True )

if __name__ == '__main__': main()
